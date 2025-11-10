{{
  config(
    materialized='view')
}}

select data_type_hk,
fullname as full_name
from {{ ref('data_type') }}