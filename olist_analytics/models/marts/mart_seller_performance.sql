with order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select * from {{ ref('fct_orders') }}
),

reviews as (
    select * from {{ ref('stg_order_reviews') }}
),

sellers as (
    select * from {{ ref('dim_sellers') }}
),

order_items_with_status as (
    select
        oi.seller_id,
        oi.order_id,
        oi.price,
        oi.freight_value,
        oi.gross_revenue,
        o.order_status,
        o.delivery_status
    from order_items oi
    left join orders o using (order_id)
),

reviews_per_seller as (
    select
        oi.seller_id,
        avg(r.review_score) as avg_review_score
    from order_items oi
    left join reviews r using (order_id)
    group by oi.seller_id
),

final as (
    select
        s.seller_id,
        s.seller_city,
        s.seller_state,
        count(distinct oi.order_id)                                         as total_orders,
        sum(oi.gross_revenue)                                               as total_revenue,
        avg(oi.price)                                                       as avg_order_price,
        count(case when oi.order_status = 'delivered' then 1 end)          as delivered_orders,
        count(case when oi.delivery_status = 'late' then 1 end)            as late_deliveries,
        round(r.avg_review_score, 2)                                        as avg_review_score
    from order_items_with_status oi
    left join sellers s using (seller_id)
    left join reviews_per_seller r using (seller_id)
    group by s.seller_id, s.seller_city, s.seller_state, r.avg_review_score
)

select * from final