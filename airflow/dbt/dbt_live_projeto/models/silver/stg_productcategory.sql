with source_data as (
    select
        productcategoryid,
        name as product_category_name,
        rowguid,
        modifieddate
    from {{ source('adventureworks-gcp', 'productcategory') }}
)
select *
from source_data