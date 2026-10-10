
## ⚙️ Requisitos previos

- Workspace de Microsoft Fabric con runtime de Spark.
- Lakehouse **Silver** adjunto al notebook: `LakehouseSilver_gus`.
- Warehouse **Gold**: `WH_gold_gus` (permisos de escritura sobre el esquema `dbo`).
- Conector Fabric Spark para Warehouse (`synapsesql`).

## 📥 Tablas de entrada (capa Silver)

| Tabla | Origen | Descripción |
|---|---|---|
| `ventas_enriquecidas` | Lakehouse Silver | Tabla de hechos base (transacciones de venta) |
| `dim_tienda` | Lakehouse Silver | Dimensión de tiendas |
| `dim_cliente` | Lakehouse Silver | Dimensión de clientes |
| `dim_empleado` | Lakehouse Silver | Dimensión de empleados |
| `dim_producto` | Lakehouse Silver | Dimensión de productos |
| `presupuesto` | Lakehouse Silver | Metas de venta mensuales por tienda |

## 📤 Tablas de salida (capa Gold → `WH_gold_gus.dbo`)

| Tabla | Contenido |
|---|---|
| `FactVentas` | Tabla de hechos enriquecida con claves, `venta_total` y `costo_total` |
| `real_vs_presupuesto` | Venta real mensualizada vs. meta, con `variacion_pct` |
| `dm_fecha` | Dimensión fecha generada para el año 2025 |
| `ventas_diarias_por_tienda` | Agregado diario de `venta_total` y `unidades` por tienda |
| `dim_tienda`, `dim_producto`, `dim_empleado`, `dim_cliente` | Dimensiones publicadas en Gold |
| `fact_ventas` | Copia de `ventas_enriquecidas` publicada en Gold |

## 🔄 Proceso paso a paso

### 1. Carga de datos
Se leen las tablas Delta desde la capa Silver con `spark.read.format("delta").table(...)`.

### 2. Construcción de `FactVentas`
- **Left joins** desde `ventas_enriquecidas` hacia las 4 dimensiones
  (`tienda_id`, `sku`, `empleado_id`, `cliente_id`).
- Se generan las claves para el modelo: `tienda_key`, `sku_key`, `empleado_key`,
  `cliente_key`.
- **Control de integridad:** la columna `marcado_revision` se setea en `1` cuando falta
  la coincidencia con tienda, producto o empleado (y en `0` en caso contrario).
- **Manejo de clientes faltantes:** `cliente_key = -1` vía `coalesce`.
- **Métricas calculadas:**
  - `venta_total = cantidad × precio_unitario × (1 − descuento_pct / 100)`
  - `costo_total` = costo unitario × cantidad

### 3. Construcción de `real_vs_presupuesto`
1. Agregación de `FactVentas` por tienda y fecha → `venta_total_real`.
2. Join con `dim_tienda` para obtener `tienda_id`.
3. Derivación de `anio`, `mes` y `anio_mes` (`yyyy/MM`).
4. Agregación mensual por tienda.
5. Left join con `presupuesto` (fecha convertida al mismo formato `yyyy/MM`).
6. Cálculo de la variación: `variacion_pct = ((venta_total_real − meta_venta) / meta_venta) × 100`.

### 4. Generación de `dm_fecha`
- Se genera el calendario completo del **año 2025** con `sequence()` + `explode()`.
- Atributos: `fecha_key` (entero `yyyyMMdd`), `fecha`, `anio`, `mes`, `trimestre`,
  `nombre_mes` y `dia_semana`, con traducción de nombres al **español**.

### 5. Agregado `ventas_diarias_por_tienda`
Agrupación por `tienda_key` y `fecha_key`, sumando `venta_total` y `cantidad` (como `unidades`).

### 6. Publicación en Gold
Todas las tablas se escriben al Warehouse con `write.mode("Overwrite").synapsesql(...)`.

## 🧮 Resumen de lógica de negocio

| Regla | Implementación |
|---|---|
| Venta total | `cantidad × precio_unitario × (1 − descuento_pct/100)` |
| Cliente desconocido | `cliente_key = -1` |
| Fila a revisar | `marcado_revision = 1` si falta tienda, producto o empleado |
| Variación vs. presupuesto | `((real − meta) / meta) × 100` |
| Periodo de la dimensión fecha | 01/01/2025 → 31/12/2025 |

## 📌 Consideraciones y mejoras futuras

- Todas las escrituras usan modo **Overwrite**: cada ejecución reemplaza el contenido
  de las tablas en Gold.
- El año de `dm_fecha` está **hardcodeado (2025)**; se recomienda parametrizarlo.
- ⚠️ **`costo_total`:** el cálculo actual obtiene un único valor de `costo_unitario`
  (`df_producto.filter(col("sku") == col("sku")).first()`), es decir, toma el costo del
  primer registro del catálogo y lo aplica a todas las filas. Se sugiere reemplazarlo
  por un join contra `dim_producto` para traer el costo correcto por `sku`.
- La tabla de fecha se publica como `dm_fecha` (verificar consistencia con el nombre
  `dim_fecha` usado en el modelo).

## ▶️ Ejecución

1. Adjuntar el Lakehouse Silver (`LakehouseSilver_gus`) al notebook.
2. Ejecutar todas las celdas en orden (**Run all**): el pipeline es secuencial,
   las secciones posteriores dependen de `FactVentas`.
3. Validar las salidas intermedias con `display()` (incluidas como controles de calidad).
