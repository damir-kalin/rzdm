{{
  config(
    materialized='view')
}}

select metric_hk,
fullname as full_name
from {{ ref('metric') }} 