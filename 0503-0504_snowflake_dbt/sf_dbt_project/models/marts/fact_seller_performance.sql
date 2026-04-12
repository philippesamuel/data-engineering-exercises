{{ config(
    materialized='table'
) }}

select
    seller_id
    , sum(price) as total_sales_value
    , count(distinct order_id) as total_sales_cnt
from {{ ref('int_order_items_sales') }}
group by 1 
order by 2,3 DESC
