{{
  config(
    materialized='view')
}}

select 
road_hk,
fullname as full_name,
id,
code,
`comment`,
sort_type
from {{ ref('road') }} 