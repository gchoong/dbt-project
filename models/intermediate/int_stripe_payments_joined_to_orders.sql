{{ config(materialized="view"
) }} 

with orders as 
(
    select * from {{ ref('stg_source__widget_shop_orders') }}
),
payments as 
(
    select * from {{ ref('stg_source__stripe_payments') }}
),
inspection_stripe_amount as 
(
    select * from {{ ref('insp_stg_source_stripe_payments') }}
),
{# remove bad data #}
inspect_amounts as 
(
    select * from payments
    where not exists (
        select 1 from inspection_stripe_amount
        where payments.id = inspection_stripe_amount.id
    )
),
join_to_orders as 
(
    select
        inspect_amounts.id as payment_id,
        inspect_amounts.payment_method as payment_method,
        inspect_amounts.status as payment_status,
        inspect_amounts.amount as payment_amount,
        inspect_amounts.created_date as payment_date,
        inspect_amounts.is_completed_payment as is_completed_payment,
        orders.id as order_id,
        orders.order_date,
        orders.status as order_status
    from inspect_amounts
    right join orders
    on inspect_amounts.orderid = orders.id
)
select * from join_to_orders
{# expected output of 121 when source table is 122. Got rid of the 1 bad row  #}