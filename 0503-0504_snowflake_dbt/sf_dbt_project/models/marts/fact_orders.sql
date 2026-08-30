{{ config(
    materialized='table'
) }}

select
    YEAR(order_purchase_timestamp) as order_purchase_year
    , order_status
    , count(distinct order_id) as cnt_distinct_order
    , count(distinct customer_id) as cnt_distinct_customer
from {{ ref('int_orders_clean') }}
group by 1,2
