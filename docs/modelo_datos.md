# Diagrama del modelo de datos

Esquema estrella en el esquema `dwh` del PostgreSQL analítico. GitHub dibuja el diagrama de abajo automáticamente. Para la versión en dbdiagram.io, importa [`modelo_datos.dbml`](modelo_datos.dbml). El DDL completo está en [`sql/ddl/`](../sql/ddl).

```mermaid
erDiagram
    dates_dim ||--o{ sales_fact : id_date
    products_dim ||--o{ sales_fact : id_product
    customers_dim ||--o{ sales_fact : id_customer
    countries_dim ||--o{ sales_fact : id_country
    dates_dim ||--o{ returns_fact : id_date
    products_dim ||--o{ returns_fact : id_product
    customers_dim ||--o{ returns_fact : id_customer
    countries_dim ||--o{ returns_fact : id_country
    dates_dim ||--o{ daily_product_revenue_agg : id_date
    products_dim ||--o{ daily_product_revenue_agg : id_product

    dates_dim {
        int id_date PK "YYYYMMDD"
        date full_date UK
        smallint year
        smallint quarter
        smallint month
        char year_month
        smallint day_of_week
        boolean is_weekend
    }
    products_dim {
        serial id_product PK
        varchar stock_code UK
        varchar product_description "nombre canónico"
        varchar category
        smallint description_variants
    }
    customers_dim {
        serial id_customer PK "-1 = ANONYMOUS"
        varchar customer_code UK
        boolean is_identified
    }
    countries_dim {
        serial id_country PK
        varchar country_name UK
    }
    sales_fact {
        char row_hash PK "MD5 clave natural"
        varchar source_system
        varchar invoice_no
        timestamptz invoice_ts_utc
        int id_date FK
        int id_product FK
        int id_customer FK
        int id_country FK
        int quantity "mayor a 0"
        numeric unit_price "mayor a 0"
        numeric gross_revenue
        char currency
    }
    returns_fact {
        char row_hash PK
        varchar source_system
        varchar invoice_no
        varchar return_type "cancellation o adjustment"
        timestamptz invoice_ts_utc
        int id_date FK
        int id_product FK
        int id_customer FK
        int id_country FK
        int quantity_returned
        numeric unit_price
        numeric return_amount
        char currency
    }
    daily_product_revenue_agg {
        int id_date PK
        int id_product PK
        int units_sold
        numeric gross_revenue
        int units_returned
        numeric returns_amount
        numeric net_revenue "bruto - devoluciones"
    }
```

## Tablas de soporte (sin relaciones con el modelo estrella)

| Esquema | Tabla | Contenido |
|---|---|---|
| `staging` | `products_stg`, `sales_stg`, `returns_stg` | Tablas `UNLOGGED` de paso para el `COPY` masivo; se vacían en cada ejecución. |
| `audit` | `rejected_records` | Fila rechazada: fuente, número de fila en el CSV, motivo y registro original en `JSONB`. |
| `audit` | `pipeline_runs` | Bitácora por ejecución: filas leídas, válidas, rechazadas, duplicadas entre fuentes y estado. |
