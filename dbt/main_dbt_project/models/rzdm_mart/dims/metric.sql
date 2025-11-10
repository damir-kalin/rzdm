{{
  config(
    materialized='table')
}}

select metric_hk,fullname from {{ ref('sat_bdv__metric') }} 
