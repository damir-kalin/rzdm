{{
  config(
    materialized='view')
}}

select medical_center_hk as med_center_hk,
fullname as full_name,
id,
code,
`comment`,
macroregion,
railway_polygon,
sort_type,
region,
financial_structure
from {{ ref('medical_center') }} 