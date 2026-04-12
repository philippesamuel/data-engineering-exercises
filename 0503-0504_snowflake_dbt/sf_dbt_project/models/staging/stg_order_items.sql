with order_items as (
    select
        order_id
        , order_item_id::INTEGER as order_item_id
        , seller_id
        , product_id
        , price::DECIMAL(10,2) as price
        , freight_value::DECIMAL(10,2) as freight_value
        , {{ cast_brt_timestamp('shipping_limit_date') }} as shipping_limit_date
    from {{ source('olist', 'OLIST_ORDER_ITEMS') }}
)
select * from order_items

