{{ config(materialized='incremental', unique_key='order_id', schema='analytics', tags=['daily']) }}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
    {% if is_incremental() %}
    WHERE order_date::date > (SELECT COALESCE(MAX(order_date::date) - INTERVAL '3 days', '1970-01-01'::DATE) FROM {{ this }})
    {% endif %}
)
SELECT
    order_id, customer_id, platform, order_date, product_id, product_name, category,
    quantity, unit_price, discount_pct,
    ROUND(quantity * unit_price,2) AS gross_revenue,
    ROUND(quantity * unit_price * discount_pct / 100.0,2) AS discount_amount,
    ROUND(quantity * unit_price * (1 - discount_pct / 100.0),2) AS net_revenue,
    shipping_cost, tax_amount,
    ROUND(quantity * unit_price * (1 - discount_pct / 100.0) + shipping_cost + tax_amount,2) AS total_amount,
    payment_method, shipping_country, shipping_state, shipping_city, is_returned,
    DATE_TRUNC('day', order_date)::DATE AS order_day,
    DATE_TRUNC('week', order_date)::DATE AS order_week,
    DATE_TRUNC('month', order_date)::DATE AS order_month,
    EXTRACT(DOW FROM order_date) AS day_of_week,
    EXTRACT(HOUR FROM order_date) AS order_hour,
    CASE WHEN EXTRACT(DOW FROM order_date) IN (0,6) THEN TRUE ELSE FALSE END AS is_weekend,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM orders