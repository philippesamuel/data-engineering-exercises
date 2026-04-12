with orders as (
    select * from {{ ref('stg_orders') }}
    where ORDER_STATUS in ('shipped', 'approved', 'delivered')
)
select * from orders

