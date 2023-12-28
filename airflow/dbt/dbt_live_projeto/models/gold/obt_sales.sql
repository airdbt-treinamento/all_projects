with
f_sales        as ( select * from {{ ref('fct_sales') }} ),
d_customer     as ( select * from {{ ref('dim_customers') }} ),
d_credit_card  as ( select * from {{ ref('dim_creditcards') }} ),
d_address      as ( select * from {{ ref('dim_address') }} ),
d_order_status as ( select * from {{ ref('dim_order_status') }} ),
d_product      as ( select * from {{ ref('dim_product') }} ),
d_calendar     as ( select * from {{ ref('dim_calendar') }} ),

final as
    (
        select
            {{ dbt_utils.star(from=ref('fct_sales'), relation_alias='f_sales', except=[
                "product_key", "customer_key", "creditcard_key", "ship_address_key", "order_status_key", "order_date_key"
            ]) }},
            {{ dbt_utils.star(from=ref('dim_product'), relation_alias='d_product', except=["product_key"]) }},
            {{ dbt_utils.star(from=ref('dim_customers'), relation_alias='d_customer', except=["customer_key"]) }},
            {{ dbt_utils.star(from=ref('dim_creditcards'), relation_alias='d_credit_card', except=["creditcard_key"]) }},
            {{ dbt_utils.star(from=ref('dim_address'), relation_alias='d_address', except=["address_key"]) }},
            {{ dbt_utils.star(from=ref('dim_order_status'), relation_alias='d_order_status', except=["order_status_key"]) }},
            {{ dbt_utils.star(from=ref('dim_calendar'), relation_alias='d_calendar', except=["date_key"]) }}
        from
            f_sales
            left join d_product on f_sales.product_key = d_product.product_key
            left join d_customer on f_sales.customer_key = d_customer.customer_key
            left join d_credit_card on f_sales.creditcard_key = d_credit_card.creditcard_key
            left join d_address on f_sales.ship_address_key = d_address.address_key
            left join d_order_status on f_sales.order_status_key = d_order_status.order_status_key
            left join d_calendar on f_sales.order_date_key = d_calendar.date_key
    )

select * from final
