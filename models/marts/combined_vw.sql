{{ config(materialized="table"
) }} 

-- Need to get customer info
with 
get_customer_orders as
(
    select * from {{ ref('int_join_orders_to_customers') }}
),  
--need to get the order amounts
get_order_amounts as 
(
    select * from {{ ref('int_stripe_payments_joined_to_orders') }}
),
-- marry the order amounts to the person 
main as
(
    select 
    get_customer_orders.order_id,
    get_customer_orders.user_id,
    get_customer_orders.full_name,
    get_customer_orders.zip,
    get_customer_orders.sold_flag,
    get_customer_orders.returned_flag,
    get_order_amounts.is_completed_payment,
    get_order_amounts.payment_method,
    get_order_amounts.order_status,
    get_order_amounts.payment_status,
    get_order_amounts.payment_amount,
    get_order_amounts.order_date,
    get_order_amounts.payment_date
    from
    get_order_amounts
    left join get_customer_orders
    on
    get_order_amounts.order_id = get_customer_orders.order_id
)
select 
*,
RANK() OVER (PARTITION BY ZIP ORDER BY payment_amount DESC) as zip_amount_rank
from main
order by payment_amount DESC