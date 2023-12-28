with source_data as (
    select
        customerid
        , personid
        , storeid
        , territoryid
    from {{ source('adventureworks-gcp', 'customer') }}
)
select *
from source_data