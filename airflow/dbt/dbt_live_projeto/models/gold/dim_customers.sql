with stg_customer as
    (
        select
            customerid,
            personid,
            storeid
        from {{ref('stg_customer')}}
    ),

stg_person as
    (
        select
            businessentityid,
            -- Adopted function concat() to concatenate first, middle and lastnames
            concat(ifnull(firstname,' '),' ',ifnull(middlename,' '),' ',ifnull(lastname,' ')) as fullname
        from {{ref('stg_person')}}
    ),

stg_store as
    (
        select
            businessentityid as storebusinessentityid,
            storename
        from
            {{ref('stg_store')}}
    ),

transformation as
    (
        select
            {{ dbt_utils.generate_surrogate_key(['stg_customer.customerid']) }} as customer_key,
            stg_customer.customerid,
            stg_person.businessentityid,
            stg_person.fullname,
            stg_store.storebusinessentityid,
            stg_store.storename,
            (
                case
                    --for every customerid, when there is no customer name related to the customerid, a store name is set
                    when stg_person.fullname is null
                    then stg_store.storename
                    else stg_person.fullname
                end
            ) as customer_fullname
        from
            stg_customer
            left join stg_person on stg_customer.personid = stg_person.businessentityid
            left join stg_store on stg_customer.storeid = stg_store.storebusinessentityid
    )

select * from transformation
