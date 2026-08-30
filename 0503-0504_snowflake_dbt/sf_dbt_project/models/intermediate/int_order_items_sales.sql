with order_items_sales as (
    select 
        seller_id
        , order_id
        , price
    from {{ ref('stg_order_items') }}
    where price is not null 
)
select * from order_items_sales

