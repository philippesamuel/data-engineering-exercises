with products as (
    select * from {{ ref('stg_products') }}
    where product_category_name is not null
)
select * from products