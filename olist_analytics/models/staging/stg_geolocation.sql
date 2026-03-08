WITH source AS (
    SELECT * FROM main.raw_geolocation
),

renamed AS (
    SELECT
        CAST(geolocation_zip_code_prefix AS VARCHAR) AS zip_code,
        geolocation_lat AS latitude,
        geolocation_lng AS longitude,
        geolocation_city AS city,
        geolocation_state AS state
    FROM source
)

SELECT * FROM renamed