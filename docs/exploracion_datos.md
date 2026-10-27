# Exploración de las fuentes

Resultados de `uv run python scripts/explore_sources.py` sobre los archivos de `data/raw/`. Cada problema encontrado tiene su tratamiento en el pipeline (columna **Decisión**).

## 1. Ficha de las fuentes

| Atributo | Ventas diarias | Histórico |
|---|---|---|
| Kaggle | carrie1/ecommerce-data | thedevastator/online-retail-transaction-dataset |
| Archivo en el repo | `data/raw/data.csv` | `data/raw/online_retail.csv` |
| `source_system` | `daily_sales` | `history` |
| Encoding | ISO-8859-1 (falla en UTF-8) | UTF-8 |
| Filas | 541.909 | 541.909 |
| Columnas | `InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country` | `index` + las mismas 8 |
| Formato de fecha | `12/1/2010 8:26` (M/D/YYYY H:MM) | `12/1/2010 8:26` |
| CustomerID | Entero (`17850`) | Decimal (`17850.0`) |
| Periodo | 01/12/2010 08:26 a 09/12/2011 12:50 | Igual |
| Granularidad | Una fila = una línea de factura | Igual |

> **Hallazgo principal.** El histórico entregado no trae dos años adicionales: es **el mismo registro** con otro formato. Las 536.480 combinaciones únicas de factura, código, cantidad y fecha están en ambos archivos. Por eso la deduplicación entre fuentes es la decisión con más impacto (ver [decisiones técnicas, 4.3](decisiones_tecnicas.md#43-solapamiento-y-duplicados-entre-las-dos-fuentes)). El pipeline también acepta el `online_retail_II.csv` original (columnas `Invoice`, `Price`, `Customer ID` y fechas ISO).

## 2. Nulos por columna (iguales en ambas fuentes)

| Columna | Nulos | % |
|---|---:|---:|
| `Description` | 1.454 | 0,27 % |
| `CustomerID` | 135.080 | **24,93 %** |
| Resto | 0 | 0 % |

## 3. Rangos y distribuciones

| Campo | Mínimo | Máximo | Comentario |
|---|---:|---:|---|
| `Quantity` | -80.995 | 80.995 | El mismo pedido de 80.995 unidades se cancela el mismo día |
| `UnitPrice` | -11.062,06 | 38.970,00 | Los negativos son ajustes `Adjust bad debt` (factura `A`) |
| Países | 38 | | Reino Unido = 495.478 filas (91,4 %) |

## 4. Problemas de calidad y decisión

| Problema | Filas (por fuente) | Decisión |
|---|---:|---|
| Cantidad ≤ 0 | 10.624 | Devolución o ajuste → `dwh.returns_fact` |
| Facturas con prefijo `C` | 9.288 | `return_type = cancellation` |
| Precio ≤ 0 | 2.517 | Si es venta (1.181): rechazo `non_positive_price`. Si es devolución con precio 0: ajuste de inventario válido |
| Sin CustomerID | 135.080 | Cliente genérico `ANONYMOUS` (`id_customer = -1`) |
| Códigos que no son producto | 2.950 | Rechazo `not_a_product_code` (POST 1.256, DOT 710, M 572, C2 144, D 77, S 63, BANK CHARGES 37, AMAZONFEE 34, CRUK 16, GIFT_0001_* 34, PADS 4...) |
| Filas duplicadas exactas | 5.268 | Rechazo `duplicate_in_source` (5.259 después de normalizar; el resto ya tenía otro motivo) |
| Códigos con más de una descripción | 645 | Nombre canónico = descripción más frecuente |
| Códigos en minúscula o con espacios | | Normalización a mayúsculas sin espacios |
| Países con alias (`EIRE`, `RSA`, `USA`, `Unspecified`) | | `seeds/country_mapping.csv` |

## 5. Resultado de la limpieza (una ejecución del DAG)

| Concepto | Filas |
|---|---:|
| Leídas (ambas fuentes) | 1.083.818 |
| Rechazadas (`audit.rejected_records`) | 18.740 (1,73 %) |
| Duplicadas entre fuentes (no se cargan) | 532.539 |
| Ventas cargadas (`dwh.sales_fact`) | 522.535 |
| Devoluciones y ajustes (`dwh.returns_fact`) | 10.004 |
| Productos (`dwh.products_dim`) | 3.927 |
| Clientes identificados (`dwh.customers_dim`) | 4.362 + ANONYMOUS |
| Países (`dwh.countries_dim`) | 38 |
| Días de calendario cubiertos (`dwh.dates_dim`) | 374 |
