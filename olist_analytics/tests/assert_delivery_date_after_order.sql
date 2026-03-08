-- Verifica que a data de entrega nunca é anterior à data do pedido
-- O teste falha se encontrar registros onde isso acontece

select
    order_id,
    order_purchase_at,
    order_delivered_customer_at
from {{ ref('fct_orders') }}
where
    order_delivered_customer_at is not null
    and order_delivered_customer_at < order_purchase_at