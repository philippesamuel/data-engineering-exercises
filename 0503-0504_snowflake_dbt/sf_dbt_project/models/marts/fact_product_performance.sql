{{ config(
    materialized='table'
) }}

select
    p.product_category_name as category
    , sum(oi.price + coalesce(oi.freight_value, 0)) as total_revenue
    , count(distinct oi.product_id) as cnt_distinct_products_sold
    , count(distinct oi.order_id) as cnt_distinct_order
    , avg(oi.order_delivery_days) as avg_delivery_days
    , avg(p.product_weight_g) as avg_product_weight_g
    , avg(p.product_photos_qty) as avg_product_photos_qty
from 
    {{ ref('int_order_items_join_orders') }} as oi
    inner join {{ ref('stg_products') }} as p 
        on (oi.product_id = p.product_id)
group by 1
