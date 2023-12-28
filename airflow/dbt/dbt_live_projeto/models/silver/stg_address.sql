with source_data as (
    select
        addressid
        , stateprovinceid
        , city
        , addressline2
        , modifieddate
        , rowguid
        , postalcode
        , spatiallocation
        , addressline1
    from {{ source('adventureworks-gcp', 'address') }}
)
select *
from source_data