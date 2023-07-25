with 

source as (

    select * from {{ ref('stg_source__stripe_payments') }}

),
final as (
select
id::string as id,
status::string as status,
orderid::string as orderid,
date_trunc('day',created_date::date) as created_date,
payment_method::string as payment_method,
amount::number(38,2) as amount,
case when status = 'success' then true else false end as is_completed_payment,
amount::number(38,2) as rejected_value,
'amount falls outside of range' as rejected_reason 
from source
where amount::number(38,2) > 100000 
{# This can be set to any number #}
)

select * from final
