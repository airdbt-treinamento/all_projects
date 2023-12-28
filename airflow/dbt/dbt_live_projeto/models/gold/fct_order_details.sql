with customers as
    (
        select
            customer_key,
            customerid
        from
            {{ref('dim_customers')}}
    ),

creditcards as
    (
        select
            creditcard_key,
            creditcardid
        from
            {{ref('dim_creditcards')}}
    ),

locations as
    (
        select
            shiptoaddress_key,
            shiptoaddressid
        from
            {{ref('dim_locations')}}
    ),


reasons as
    (
        select
            salesorder_key,
            salesorderid,
            reason_name_aggregated
        from
            {{ref('dim_reasons')}}
    ),

product as
    (
        select
            product_key,
            productid
        from
            {{ref('dim_product')}}
    ),

salesorderdetail as
    (
        select
            stg_salesorderdetail.salesorderid,
            product.product_key as product_fk,
            stg_salesorderdetail.productid,
            stg_salesorderdetail.orderqty,
            stg_salesorderdetail.unitprice,
            stg_salesorderdetail.unitprice * stg_salesorderdetail.orderqty  AS  revenue_wo_taxandfreight,
            -- Sales reason (a dimension) was attached to the fact table due to a data studio limit on allowed merges
            -- Attributing 'Not indicated' if there is no sales reason indicated
            ifnull(reasons.reason_name_aggregated,'Not indicated') as reason_name_final
        from
            {{ref('stg_salesorderdetail')}} stg_salesorderdetail
            left join product on stg_salesorderdetail.productid = product.productid
            left join reasons on stg_salesorderdetail.salesorderid = reasons.salesorderid
    ),

salesorderheader as
    (
        select
            salesorderid,
            customers.customer_key as customer_fk,
            creditcards.creditcard_key as creditcard_fk,
            locations.shiptoaddress_key as shiptoadress_fk,
            -- Description added to order_status based on column descriptions in PostgreSQL.
            (
                CASE
                    WHEN order_status = 1 THEN 'In_process'
                    WHEN order_status = 2 THEN 'Approved'
                    WHEN order_status = 3 THEN 'Backordered'
                    WHEN order_status = 4 THEN 'Rejected'
                    WHEN order_status = 5 THEN 'Shipped'
                    WHEN order_status = 6 THEN 'Cancelled'
                    ELSE 'no_status'
                end
            ) as order_status_name,
            CAST(orderdate AS DATE) AS orderdate
        from
            {{ref('stg_salesorderheader')}}
            left join customers on stg_salesorderheader.customerid = customers.customerid
            left join creditcards on stg_salesorderheader.creditcardid = creditcards.creditcardid
            left join locations on stg_salesorderheader.shiptoaddressid = locations.shiptoaddressid
    ),

/* We then join salesorderdetail and salesorderheader to get the final fact table*/
final as
    (
        select
            salesorderdetail.salesorderid,
            salesorderdetail.product_fk,
            salesorderheader.customer_fk,
            salesorderheader.shiptoadress_fk,
            salesorderheader.creditcard_fk,
            salesorderdetail.unitprice,
            salesorderdetail.orderqty,
            salesorderdetail.revenue_wo_taxandfreight,
            salesorderdetail.reason_name_final,
            salesorderheader.orderdate,
            salesorderheader.order_status_name
        from
            salesorderdetail
            left join salesorderheader on salesorderdetail.salesorderid = salesorderheader.salesorderid
    )

select * from final
