WITH source AS (
    SELECT * FROM main.raw_product_category_name_translation
),

renamed AS (
    SELECT
        product_category_name,
        product_category_name_english as product_category_name_en
    FROM source
)

SELECT * FROM renamed