with stg_salesorderheader as
    (
        select
            distinct(shiptoaddressid)
        from {{ref('stg_salesorderheader')}}
    ),

stg_address as
    (
        select *
        from {{ref('stg_address')}}
    ),

stg_stateprovince as
    (
        select *
        from {{ref('stg_stateprovince')}}
    ),

stg_countryregion as
    (
        select *
        from {{ref('stg_countryregion')}}
    ),

transformation as
    (
        select
            {{ dbt_utils.generate_surrogate_key(['stg_salesorderheader.shiptoaddressid']) }} as shiptoaddress_key,
            stg_salesorderheader.shiptoaddressid,
            stg_address.city as city_name,
            stg_stateprovince.state_name,
            stg_countryregion.country_name
        from
            stg_salesorderheader
            left join stg_address on stg_salesorderheader.shiptoaddressid = stg_address.addressid
            left join 	stg_stateprovince on stg_address.stateprovinceid = stg_stateprovince.stateprovinceid
            left join stg_countryregion on stg_stateprovince.countryregioncode = stg_countryregion.countryregioncode
    )

select * from transformation
