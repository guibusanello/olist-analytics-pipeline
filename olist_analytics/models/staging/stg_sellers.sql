WITH source AS (
    SELECT * FROM main.raw_sellers
),

renamed AS (
    SELECT
        seller_id,
        CAST(seller_zip_code_prefix AS VARCHAR) AS seller_zip_code,
        seller_city,
        UPPER(seller_state) AS seller_state
    FROM source
)

SELECT * FROM renamed