{{ 
    config(
        materialized='incremental', 
        unique_key='order_id', 
        schema='staging', 
        tags=['daily']
    ) 
}}

WITH source AS (
    SELECT * FROM {{ source('staging', 'daily_orders') }}
    {% if is_incremental() %}
    -- Only process records newer than the latest we already have
    -- The -3 day buffer catches any late-arriving records from recent days
    WHERE etl_processed_at > (
        SELECT COALESCE(MAX(etl_processed_at) - INTERVAL '3 days', '1970-01-01'::TIMESTAMP) 
        FROM {{ this }}
    )
    {% endif %}
),

cleaned AS (
    SELECT
        order_id, 
        customer_id, 
        LOWER(TRIM(platform)) AS platform, 
        order_date,
        product_id, TRIM(product_name) AS product_name, 
        TRIM(category) AS category,
        quantity, ROUND(unit_price::NUMERIC,2) AS unit_price,
        COALESCE(discount_pct,0) AS discount_pct,
        COALESCE(shipping_cost,0) AS shipping_cost,
        COALESCE(tax_amount,0) AS tax_amount,
        UPPER(TRIM(payment_method)) AS payment_method,
        UPPER(TRIM(shipping_country)) AS shipping_country,
        TRIM(shipping_state) AS shipping_state,
        TRIM(shipping_city) AS shipping_city,
        COALESCE(is_returned, FALSE) AS is_returned,
        etl_processed_at
    FROM source
    WHERE order_id IS NOT NULL 
        AND customer_id IS NOT NULL 
        AND quantity > 0
)

SELECT * FROM cleaned