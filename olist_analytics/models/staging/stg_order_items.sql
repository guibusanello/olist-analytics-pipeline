WITH source AS (
    SELECT * FROM main.raw_order_items
),

renamed AS (
    SELECT
        order_id,
        order_item_id,
        product_id,
        seller_id,
        CAST(shipping_limit_date AS TIMESTAMP) AS shipping_limit_at,
        price,
        freight_value,
        price + freight_value AS gross_revenue
    FROM source
)

SELECT * FROM renamed