with orders as (
    select
        order_id
        , customer_id
        , order_status
        , cast(order_purchase_timestamp as timestamp) as order_purchase_timestamp
        , cast(order_approved_at as timestamp) as order_approved_at
        , cast(order_delivered_carrier_date as timestamp) as order_delivered_carrier_date
        , cast(order_delivered_customer_date as timestamp) as order_delivered_customer_date
    from {{ source('olist', 'OLIST_ORDERS') }}
)
select * from orders

