#!/usr/bin/env python
# coding: utf-8

# ## Notebook_Silver_Gold_DEF
# 
# null

# ###  gold.FactVentas

# In[1]:


# Welcome to your new notebook
# Type here in the cell editor to add code!
df_ventas_enr = spark.read.format("delta").table("ventas_enriquecidas")
df_tienda = spark.read.format("delta").table("dim_tienda")
df_cliente = spark.read.format("delta").table("dim_cliente")
df_empleado = spark.read.format("delta").table("dim_empleado")
df_producto = spark.read.format("delta").table("dim_producto")


# In[2]:


from pyspark.sql.functions import col, when, lit, coalesce

# 1. Realizar la secuencia de LEFT JOINS
df_transformado = df_ventas_enr \
    .join(df_tienda, on="tienda_id", how="left") \
    .join(df_producto, on="sku", how="left") \
    .join(df_empleado, on="empleado_id", how="left") \
    .join(df_cliente, on="cliente_id", how="left")

# 2. Seleccionar columnas, aplicar lógica de negocio y marcar para revisión
df_resultado = df_transformado.select(
    # Mantiene todas las columnas de la tabla de hechos original (ventas)
    df_ventas_enr["*"],
    
    # Trae las claves de las dimensiones directamente (quedarán NULL si no hay coincidencia)
    df_empleado["empleado_id"].alias("empleado_key"), 
    coalesce(df_cliente["cliente_id"], lit(-1)).alias("cliente_key"),
    df_tienda["tienda_id"].alias("tienda_key"), 
    df_producto["sku"].alias("sku_key"),
   
    # LOGICA DE REVISIÓN: Si falta tienda, producto O empleado, se marca con 1 (o True), si no 0
    when(
        df_tienda["tienda_id"].isNull() | 
        df_producto["sku"].isNull() | 
        df_empleado["empleado_id"].isNull(), 
        lit(1)  # Puedes usar True si prefieres un tipo booleano
    ).otherwise(lit(0)).alias("marcado_revision")
)


# 

# In[3]:


# Crea df_final2 agregando únicamente la columna calculada venta_total
df_final2 = df_resultado \
    .withColumn(
        "venta_total", 
        col("cantidad") * col("precio_unitario") * 
        (lit(1) - coalesce(col("descuento_pct"), lit(0)) / lit(100)))\
    .withColumn(
        "costo_total", 
        int(df_producto.filter(col("sku") == col("sku"))
        .select("costo_unitario").first()["costo_unitario"])*
         coalesce(col("cantidad"), lit(0))
    )


# In[4]:


#df_final2.filter(col("cliente_id").isNull()).show(10)


# In[5]:


FactVentas= df_final2.select(
    "transaccion_id", 
    col("fecha").alias("fecha_key"),
    "tienda_key", 
    "sku_key", 
    "empleado_key", 
    "cliente_key", 
    "cantidad", 
    "precio_unitario",
    "descuento_pct", 
    "venta_total", 
    "costo_total"
)


# In[6]:


#FactVentas.filter(col("cliente_id").isNull()).show(1)


# ### se pasa a gold => WH_gold_gus

# In[7]:


import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants

#df_wh = spark.read.synapsesql("WH_gold_gus.dbo.table1")
FactVentas.write.mode("Overwrite").synapsesql("WH_gold_gus.dbo.FactVentas")


# ### gold. real_vs_presupuesto

# In[8]:


from pyspark.sql.functions import sum

# Paso 1: Agrupar por tienda y fecha, sumando la venta_total
paso1_agregado = FactVentas.groupBy("tienda_key", "fecha_key") \
    .agg(sum("venta_total").alias("venta_total_real"))

# Mostrar el resultado para verificar que esté correcto
display(paso1_agregado.limit(5))


# In[9]:


# 1. Leer la tabla DimTienda (Agregué el .dbo que usaste)
dim_tienda = spark.read.table("LakehouseSilver_gus.dbo.dim_tienda")

# 2. CÓDIGO CORREGIDO: Cruzar vinculando 'tienda_key' con 'tienda_id' de la dimensión
paso2_tienda = paso1_agregado.join(dim_tienda, paso1_agregado.tienda_key == dim_tienda.tienda_id, "inner") \
    .select("fecha_key", "tienda_id", "venta_total_real")

# Mostrar el resultado para verificar
display(paso2_tienda.limit(5))


# In[10]:


from pyspark.sql.functions import year, month, concat, lit, lpad, col

# Paso 3: Extraer año/mes de 'fecha_key' y construir 'anio_mes' (YYYY/MM)
paso3_fecha = paso2_tienda \
    .withColumn("anio", year(col("fecha_key"))) \
    .withColumn("mes", month(col("fecha_key"))) \
    .withColumn("anio_mes", concat(col("anio"), lit("/"), lpad(col("mes"), 2, "0")))

# Mostrar el resultado para verificar las nuevas columnas
display(paso3_fecha.select("fecha_key", "anio", "mes", "anio_mes", "venta_total_real").limit(5))


# In[11]:


from pyspark.sql.functions import sum

# Agrupamos por tienda y mes para tener un único total mensualizado
paso3_mensual = paso3_fecha.groupBy("tienda_id", "anio_mes") \
    .agg(sum("venta_total_real").alias("venta_total_real"))

# Verificar el resultado consolidado
display(paso3_mensual.limit(5))


# In[12]:


df = spark.sql("SELECT * FROM LakehouseSilver_gus.dbo.presupuesto LIMIT 2")
display(df)


# In[13]:


from pyspark.sql.functions import date_format

# 1. Leer la tabla de presupuestos
silver_presupuesto_crudo = spark.read.table("LakehouseSilver_gus.dbo.presupuesto")

# 2. CÓDIGO CORREGIDO: Convertir la fecha real "2025-03-01" al texto "2025/03"
silver_presupuesto = silver_presupuesto_crudo.withColumn(
    "anio_mes", 
    date_format(col("anio_mes"), "yyyy/MM")
)

# 3. Paso 4: Hacer el Left Outer Join (Ahora los formatos de texto coinciden perfectamente)
paso4_presupuesto = paso3_mensual.join(
    silver_presupuesto, 
    ["tienda_id", "anio_mes"], 
    "left"
)

# 4. Paso 5: Calcular la variacion_pct
paso5_variacion = paso4_presupuesto.withColumn(
    "variacion_pct", 
    ((col("venta_total_real") - col("meta_venta")) / col("meta_venta")) * 100
)

# 5. Conservar solo los campos finales solicitados
resultado_final = paso5_variacion.select(
    "tienda_id", 
    "anio_mes", 
    "venta_total_real", 
    "meta_venta", 
    "variacion_pct"
)

# Mostrar el resultado final corregido
display(resultado_final.limit(5))


# se Pasa GOLD  puente_real_vs_presupuesto

# In[14]:


import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants


resultado_final.write.mode("Overwrite").synapsesql("WH_gold_gus.dbo.real_vs_presupuesto")


# In[15]:


from pyspark.sql.functions import explode, sequence, to_date, lit, col, date_format, year, month, quarter, when

# 1. Crear el rango de fechas para todo el año 2025 (desde el 1 de enero al 31 de diciembre)
df_fechas_base = spark.createDataFrame([( "2025-01-01", "2025-12-31" )], ["inicio", "fin"])

df_dim_fecha = df_fechas_base.select(
    explode(sequence(to_date(col("inicio")), to_date(col("fin")))).alias("fecha")
)

# 2. Calcular los atributos requeridos por la consigna
df_dim_fecha_final = df_dim_fecha \
    .withColumn("fecha_key", date_format(col("fecha"), "yyyyMMdd").cast("integer")) \
    .withColumn("anio", year(col("fecha"))) \
    .withColumn("mes", month(col("fecha"))) \
    .withColumn("trimestre", quarter(col("fecha"))) \
    .withColumn("nombre_mes_en", date_format(col("fecha"), "MMMM")) \
    .withColumn("dia_semana_en", date_format(col("fecha"), "EEEE"))

# 3. Traducir los nombres de meses a Español (Enero, Febrero, etc.)
df_dim_fecha_final = df_dim_fecha_final.withColumn("nombre_mes", 
    when(col("nombre_mes_en") == "January", "Enero")
    .when(col("nombre_mes_en") == "February", "Febrero")
    .when(col("nombre_mes_en") == "March", "Marzo")
    .when(col("nombre_mes_en") == "April", "Abril")
    .when(col("nombre_mes_en") == "May", "Mayo")
    .when(col("nombre_mes_en") == "June", "Junio")
    .when(col("nombre_mes_en") == "July", "Julio")
    .when(col("nombre_mes_en") == "August", "Agosto")
    .when(col("nombre_mes_en") == "September", "Septiembre")
    .when(col("nombre_mes_en") == "October", "Octubre")
    .when(col("nombre_mes_en") == "November", "Noviembre")
    .when(col("nombre_mes_en") == "December", "Diciembre")
)

# 4. Traducir los días de la semana a Español (Lunes, Martes, etc.)
df_dim_fecha_final = df_dim_fecha_final.withColumn("dia_semana", 
    when(col("dia_semana_en") == "Monday", "Lunes")
    .when(col("dia_semana_en") == "Tuesday", "Martes")
    .when(col("dia_semana_en") == "Wednesday", "Miércoles")
    .when(col("dia_semana_en") == "Thursday", "Jueves")
    .when(col("dia_semana_en") == "Friday", "Viernes")
    .when(col("dia_semana_en") == "Saturday", "Sábado")
    .when(col("dia_semana_en") == "Sunday", "Domingo")
)

# 5. Seleccionar y ordenar las columnas exactamente como pide la estructura final
dim_fecha_lista = df_dim_fecha_final.select(
    "fecha_key",
    "fecha",
    "anio",
    "mes",
    "trimestre",
    "nombre_mes",
    "dia_semana"
).orderBy("fecha_key")

# Mostrar el resultado final generado
display(dim_fecha_lista.limit(5))


# Se exporta a Gold a la Otra tabla generada.

# In[16]:


import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants

#df_wh = spark.read.synapsesql("WH_gold_gus.dbo.table1")
dim_fecha_lista.write.mode("Overwrite").synapsesql("WH_gold_gus.dbo.dm_fecha")


# ### gold.ventas_diarias_por_tienda — venta_total y unidades agregadas

# In[17]:


from pyspark.sql.functions import sum

# 1. Agrupar FactVentas por tienda y día, sumando venta_total y cantidad
df_ventas_diarias = FactVentas.groupBy("tienda_key", "fecha_key") \
    .agg(
        sum("venta_total").alias("venta_total"),
        sum("cantidad").alias("unidades")
    )





# Mostrar una vista previa para validar
display(df_ventas_diarias.limit(100))


# ### se graba tabla gold.ventas_diarias_por_tienda

# In[18]:


import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants

#df_wh = spark.read.synapsesql("WH_gold_gus.dbo.table1")
df_ventas_diarias.write.mode("Overwrite").synapsesql("WH_gold_gus.dbo.ventas_diarias_por_tienda")


# In[19]:


##Se guardan las tablas como figura en el schema GOLD

df_tienda.write.mode("Overwrite").synapsesql("WH_gold_gus.dbo.dim_tienda")
df_producto.write.mode("Overwrite").synapsesql("WH_gold_gus.dbo.dim_producto")
df_empleado.write.mode("Overwrite").synapsesql("WH_gold_gus.dbo.dim_empleado")
df_cliente.write.mode("Overwrite").synapsesql("WH_gold_gus.dbo.dim_cliente")
df_ventas_enr.write.mode("Overwrite").synapsesql("WH_gold_gus.dbo.fact_ventas")




# 
