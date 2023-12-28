with source_data as
    (
        select
            salesorderid,
            modifieddate,
            salesreasonid
        from
            {{ source('adventureworks-gcp', 'salesorderheadersalesreason') }}
    )

select * from source_data
