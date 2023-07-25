with 

source as (

    select * from {{ source('source', 'widget_shop_customers') }}

),

renamed as (

    select
        id::varchar as id,    
        first_name::string as first_name,
        last_name::string as last_name,
        concat(first_name, ' ',last_name) as full_name,
        zip::string as zip
    from source

)

select * from renamed
