WITH source AS (
    SELECT * FROM main.raw_order_reviews
),

renamed AS (
    SELECT
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        CAST(review_creation_date AS TIMESTAMP) AS review_creation_at,
        CAST(review_answer_timestamp AS TIMESTAMP) AS review_answer_at
    FROM source
)

SELECT * FROM renamed