{{ config(materialized="view"
) }} 
with
highest_order  as 
(
    select
        zip,
        AVG(payment_amount) as average_payment,
        Sum(payment_amount) as total_amount,
        median(payment_amount) as median_amount,
        count(order_id) as order_count
    from
        {{ ref('combined_vw') }}
    where  
        sold_flag = 1 
        and
        is_completed_payment = 1
    group by 
        1
)
select 
*,
case when average_payment = total_amount/order_count then 'correct' else 'wrong' end as "check" 
from highest_order
order by total_amount DESC
