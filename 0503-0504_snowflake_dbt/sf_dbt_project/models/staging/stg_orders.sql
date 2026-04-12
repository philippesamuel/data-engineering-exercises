with orders as (
    select
        order_id
        , customer_id
        , order_status
        , {{ cast_brt_timestamp('order_purchase_timestamp') }} as order_purchase_timestamp
        , {{ cast_brt_timestamp('order_approved_at') }} as order_approved_at
        , {{ cast_brt_timestamp('order_delivered_carrier_date') }} as order_delivered_carrier_date
        , {{ cast_brt_timestamp('order_delivered_customer_date') }} as order_delivered_customer_date
        , {{ cast_brt_timestamp('order_estimated_delivery_date') }} as order_estimated_delivery_date
    from {{ source('olist', 'OLIST_ORDERS') }}
)
select * from orders

