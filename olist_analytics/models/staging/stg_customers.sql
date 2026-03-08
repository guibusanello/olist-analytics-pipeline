WITH source AS (
    SELECT * FROM main.raw_customers
),

renamed AS (
    SELECT
        customer_id,
        customer_unique_id,
        CAST(customer_zip_code_prefix as VARCHAR) as customer_zip_code,
        customer_city,
        UPPER(customer_state) as customer_state
    FROM source
)

SELECT * FROM renamed