{{ config(materialized="view"
) }} 
with 
"input" as (
select 
SUM(CASE WHEN returned_flag = 1 THEN 1 ELSE 0 END) AS count_of_returns,
SUM(CASE WHEN sold_flag = 1 THEN 1 ELSE 0 END) AS count_of_sold
from {{ ref('combined_vw') }}
),
"output" as 
(
    select 
    count_of_returns/count_of_sold as math
    from "input"
)
select math::decimal(38,4) as division from "output"
