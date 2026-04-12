{{ config(
    materialized='table'
) }}

select
    YEAR(purchase_date) as year
    , order_status
    , customer_state
    , customer_city
    , count(distinct order_id) as ctn_distinct_orders
    , count(distinct customer_unique_id) as cnt_distinct_customers
from {{ ref('int_orders_join_customers') }}
group by 1,2,3,4 
order by 1,2,3,4
