with 

source as (

    select * from {{ source('source', 'stripe_payments') }}

),

renamed as (

select
id::string as id,
status::string as status,
orderid::string as orderid,
date_trunc('day',created::date) as created_date,
payment_method::string as payment_method,
amount::number(38,2) as amount,
case when status = 'success' then true else false end as is_completed_payment  
from source

)

select * from renamed
