{{
  config(
    materialized='view')
}}

select value_type_hk,
fullname as full_name
from {{ ref('value_type') }} 