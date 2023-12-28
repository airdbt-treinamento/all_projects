with source_data as (
    select
        salesreasonid
        , name as reason_name
        , reasontype
        , modifieddate
    from {{ source('adventureworks-gcp', 'salesreason') }}
)
select *
from source_data