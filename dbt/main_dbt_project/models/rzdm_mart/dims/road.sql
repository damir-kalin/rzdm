{{
  config(
    materialized='table')
}}

select road_hk,fullname,id,code,`comment`,sort_type from {{ ref('sat_bdv__road') }}
