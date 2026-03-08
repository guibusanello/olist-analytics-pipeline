WITH orders as (
    SELECT * FROM {{ ref('stg_orders') }}
),

final AS (
    SELECT
        order_id,
        order_purchase_at,
        order_delivered_customer_at,
        order_estimated_delivery_at,
        -- dias para entregar ao cliente
        datediff('day', order_purchase_at, order_delivered_customer_at)    AS days_to_deliver,
        -- dias estimados para entrega
        datediff('day', order_purchase_at, order_estimated_delivery_at)    AS days_estimated,
        -- diferença entre estimado e real (positivo = atrasou, negativo = adiantou)
        datediff('day', order_estimated_delivery_at, order_delivered_customer_at) AS delivery_delay_days,
        CASE
            WHEN order_delivered_customer_at <= order_estimated_delivery_at THEN 'on_time'
            WHEN order_delivered_customer_at >  order_estimated_delivery_at THEN 'late'
            ELSE 'unknown'
        END AS delivery_status
    FROM orders
    WHERE order_delivered_customer_at IS NOT NULL
)

SELECT * FROM final