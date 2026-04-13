with customers as (
    select
        customer_id
        , customer_unique_id
        , customer_zip_code_prefix::INTEGER as customer_zip_code_prefix
        , customer_city
        , customer_state
    from {{ source('olist', 'OLIST_CUSTOMERS') }}
)
select * from customers

