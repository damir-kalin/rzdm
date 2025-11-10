{{
  config(
    materialized='table')
}}

select data_type_hk,fullname from {{ ref('sat_bdv__data_type') }} 
