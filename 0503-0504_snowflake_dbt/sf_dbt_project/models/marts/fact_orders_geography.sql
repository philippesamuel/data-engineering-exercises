{{ config(
    materialized='table'
) }}

select
    YEAR(order_purchase_date) as order_purchase_year
    , order_status
    , customer_state
    , customer_city
    , count(distinct order_id) as cnt_distinct_order
    , count(distinct customer_unique_id) as cnt_distinct_customer
from {{ ref('int_orders_join_customers') }}
group by 1,2,3,4 
order by 1,2,3,4
