{{
  config(
    materialized='table')
}}

select value_type_hk,fullname from {{ ref('sat_bdv__value_type') }}
