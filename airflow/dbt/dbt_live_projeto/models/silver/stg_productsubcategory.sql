with source_data as (
    select
        productsubcategoryid,
        productcategoryid,
        name as product_subcategory_name,
        rowguid,
        modifieddate
    from {{ source('adventureworks-gcp', 'productsubcategory') }}
)
select *
from source_data