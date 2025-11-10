{{
  config(
    materialized='table')
}}

select medical_center_hk,fullname,id,code,`comment`,macroregion,railway_polygon,sort_type,region,financial_structure from {{ ref('sat_bdv__medical_center') }}
