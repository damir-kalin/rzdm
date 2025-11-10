{{
  config(
    materialized='view')
}}

select unit_hk,
fullname as full_name
from {{ ref('unit') }} 