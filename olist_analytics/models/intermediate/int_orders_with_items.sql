WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

order_items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
),

order_aggregated AS (
    SELECT
        order_id,
        COUNT(order_item_id) AS total_items,
        SUM(price) AS total_price,
        SUM(freight_value) AS total_freight,
        SUM(gross_revenue) AS total_gross_revenue
    FROM order_items
    GROUP BY order_id
),

final AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.order_status,
        o.order_purchase_at,
        o.order_approved_at,
        o.order_delivered_carrier_at,
        o.order_delivered_customer_at,
        o.order_estimated_delivery_at,
        oi.total_items,
        oi.total_price,
        oi.total_freight,
        oi.total_gross_revenue
    FROM orders o
    LEFT JOIN order_aggregated oi USING (order_id)
)

SELECT * FROM final
