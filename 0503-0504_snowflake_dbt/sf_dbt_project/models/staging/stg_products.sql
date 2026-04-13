with products as 
    select
        -- ids
        product_id::VARCHAR                     as product_id
        
        -- product metadata
        , product_category_name::VARCHAR        as product_category_name
        , product_name_length::INTEGER          as product_name_length
        , produce_description_length::INTEGER   as produce_description_length
        , product_photos_qty::INTEGER           as product_photos_qty
        
        -- physical dimensions
        , product_weight_g::INTEGER             as product_weight_g
        , product_length_cm::INTEGER            as product_length_cm
        , product_height_cm::INTEGER            as product_height_cm
        , product_width_cm::INTEGER             as product_width_cm

    from {{ source('olist', 'OLIST_PRODUCTS') }}
)
select * from products

