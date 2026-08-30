{{ config(
    materialized='table'
) }}

select
    seller_id
    , sum(price) as total_sales_value
    , count(distinct order_id) as order_count
from {{ ref('int_order_items_sales') }}
group by 1 
