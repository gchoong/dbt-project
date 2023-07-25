with

source as (

    select * from {{ source('source', 'widget_shop_orders') }}

),

renamed as (

    select
        id::varchar as id,
        user_id::string as user_id,
        order_date::date as order_date,
        case
            when status in ('plced') then 'placed' else status
        end as status,
        case
            when
                status in
                (
                    'returned',
                    'return_pending'
                )
                then 1
                else 0
            end as returned_flag,
        case    
            when
                status in
                (
                    'completed',
                    'shipped',
                    'placed',
                    'plced' -- To account for bad data, I normally would like to handle this before it gets landed into the warehouse? Could also handle it within the def layers
                )
                then 1
                else 0 
        end as sold_flag,
        case 
            when 
                status is null
                or 
                status = 'null'
            then
            1
            else
            0
            end as 
            invalid_flag
    from source

)

select * from renamed
