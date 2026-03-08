with orders as (
    select * from {{ ref('fct_orders') }}
),

monthly as (
    select
        date_trunc('month', order_purchase_at)  as order_month,
        count(distinct order_id)                as total_orders,
        count(distinct customer_id)             as total_customers,
        sum(total_gross_revenue)                as total_revenue,
        avg(total_gross_revenue)                as avg_order_revenue
    from orders
    where order_status = 'delivered'
    group by date_trunc('month', order_purchase_at)
),

final as (
    select
        order_month,
        total_orders,
        total_customers,
        round(total_revenue, 2)                 as total_revenue,
        round(avg_order_revenue, 2)             as avg_order_revenue,
        round(lag(total_revenue) over (
            order by order_month
        ), 2)                                   as prev_month_revenue,
        round(total_revenue - lag(total_revenue) over (
            order by order_month
        ), 2)                                   as revenue_change,
        round(100.0 * (total_revenue - lag(total_revenue) over (
            order by order_month
        )) / nullif(lag(total_revenue) over (
            order by order_month
        ), 0), 2)                               as revenue_growth_pct
    from monthly
)

select * from final
order by order_month