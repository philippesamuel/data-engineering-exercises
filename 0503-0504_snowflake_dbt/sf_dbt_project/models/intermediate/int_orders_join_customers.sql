with orders_join_customers as (
    select
        o.order_id
        , o.customer_id
        , o.order_status
        , DATE(o.order_purchase_timestamp) as order_purchase_date
        , c.customer_unique_id
        , c.customer_city
        , c.customer_state
    from 
        {{ ref('stg_orders') }} as o
        inner join {{ ref('stg_customers' )}} as c 
            on (o.customer_id = c.customer_id)
    where 
        order_status in ('shipped', 'approved', 'delivered')
)
select * from orders_join_customers
