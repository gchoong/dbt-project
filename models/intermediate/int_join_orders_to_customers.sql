{{ config(materialized="view"
) }} 

with orders as 
(
    select * from {{ ref('stg_source__widget_shop_orders') }}
),
customers as 
(
    select * from {{ ref('stg_source__widget_shop_customers') }}
),
join_to_customers as 
(
    select
        orders.id as order_id,
        orders.sold_flag as sold_flag,
        orders.returned_flag as returned_flag,
        customers.id as user_id,
        customers.full_name,
        customers.zip as zip
    from orders
    right join customers
    on orders.user_id = customers.id
)
select * from join_to_customers
where order_id is not null