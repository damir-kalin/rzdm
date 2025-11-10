{{
  config(
    materialized='table')
}}

select unit_hk,fullname from {{ ref('sat_bdv__unit') }}
