{{
  config(
    materialized='view')
}}

select discreteness_hk,
fullname as full_name
from {{ ref('discreteness') }} 