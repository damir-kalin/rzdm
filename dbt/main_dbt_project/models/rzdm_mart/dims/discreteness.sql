{{
  config(
    materialized='table')
}}

select discreteness_hk,fullname from {{ ref('sat_bdv__discreteness') }}
