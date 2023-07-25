{{ config(materialized="view"
) }} 
with
average_order_per_customer as 
(
    select
        user_id,
        full_name,
        AVG(payment_amount) as average_payment,
        Sum(payment_amount) as total_amount,
        count(order_id) as order_count
    from
        {{ ref('combined_vw') }}
    where  
        sold_flag = 1 
        and
        is_completed_payment = 1
    group by 
        1,2
)
select 
*,
case when average_payment = total_amount/order_count then 'correct' else 'wrong' end as "check" 
from average_order_per_customer
