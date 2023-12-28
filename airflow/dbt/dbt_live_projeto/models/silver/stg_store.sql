with source_data as (
    select
        businessentityid
        , name as storename
        , salespersonid
        , modifieddate
    from {{ source('adventureworks-gcp', 'store') }}
)
select *
from source_data