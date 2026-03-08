with orders as (
    select * from {{ ref('int_orders_with_items') }}
),

delivery as (
    select * from {{ ref('int_delivery_times') }}
),

payments as (
    select
        order_id,
        sum(payment_value)                    as total_payment_value,
        count(distinct payment_type)          as distinct_payment_types,
        max(payment_installments)             as max_installments
    from {{ ref('stg_order_payments') }}
    group by order_id
),

reviews as (
    select
        order_id,
        avg(review_score)                     as avg_review_score
    from {{ ref('stg_order_reviews') }}
    group by order_id
),

final as (
    select
        o.order_id,
        o.customer_id,
        o.order_status,
        o.order_purchase_at,
        o.order_approved_at,
        o.order_delivered_customer_at,
        o.order_estimated_delivery_at,
        o.total_items,
        o.total_price,
        o.total_freight,
        o.total_gross_revenue,
        p.total_payment_value,
        p.distinct_payment_types,
        p.max_installments,
        d.days_to_deliver,
        d.days_estimated,
        d.delivery_delay_days,
        d.delivery_status,
        r.avg_review_score
    from orders o
    left join delivery  d using (order_id)
    left join payments  p using (order_id)
    left join reviews   r using (order_id)
)

select * from final