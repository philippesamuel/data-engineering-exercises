with orders as (
    select * from {{ ref('stg_orders') }}
    where order_status in ('shipped', 'approved', 'delivered')
)
select * from orders

