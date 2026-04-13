with order_items_join_orders as (
    select
        -- ids
        oi.order_id
        , oi.product_id

        -- commented out for now
        -- uncomment if needed downstream
        -- , oi.order_item_id
        -- , oi.seller_id
        -- , o.customer_id

        -- monetary 
        , oi.price
        , oi.freight_value

        -- order info
        , o.order_status
        , o.order_purchase_timestamp::DATE          as order_purchase_date
        , o.order_delivered_customer_date::DATE     as order_delivered_customer_date
        
        -- , o.order_approved_at::DATE              as order_approved_at
        -- , o.order_delivered_carrier_date::DATE   as order_delivered_carrier_date
        -- , o.order_estimated_delivery_date ::DATE as order_estimated_delivery_date
        -- , oi.shipping_limit_date
    from 
        {{ ref('stg_order_items') }} as oi
        inner join {{ ref('stg_orders') }} as o
            on (oi.order_id = o.order_id)
    where 
        o.order_status in ('shipped', 'approved', 'delivered')
)
select 
    *
    -- NULL for shipped/approved rows since delivery hasn't happened yet
    , (order_delivered_customer_date - order_purchase_date) as order_delivery_days 
from order_items_join_orders
