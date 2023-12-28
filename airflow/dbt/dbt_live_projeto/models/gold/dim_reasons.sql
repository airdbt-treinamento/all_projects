with stg_salesorderheadersalesreason as
    (
        select *
        from {{ref('stg_salesorderheadersalesreason')}}
    ),

stg_salesreason as
    (
        select *
        from {{ref('stg_salesreason')}}
    ),

reasonbyorderid as
    (
        select
            stg_salesorderheadersalesreason.salesorderid,
            stg_salesreason.reason_name as reason_name
        from
            stg_salesorderheadersalesreason
            left join stg_salesreason on stg_salesorderheadersalesreason.salesreasonid = stg_salesreason.salesreasonid
    ),

transformation as
    (
        select
            {{ dbt_utils.generate_surrogate_key(['reasonbyorderid.salesorderid']) }} as salesorder_key,
            salesorderid,
            -- function used to aggregate in one row any multiple reasons attributed to a single salesorderid
            string_agg(reason_name, ', ') as reason_name_aggregated
        from
            reasonbyorderid
        group by
            salesorderid
    )

select * from transformation
