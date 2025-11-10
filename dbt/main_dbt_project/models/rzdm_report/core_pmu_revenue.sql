{{
  config(
    materialized='view')
}}



select 
`date`,
road_hk,
medical_center_hk as med_center_hk,
value_type_hk,
discreteness_hk,
unit_hk,
metric_hk,
data_type_hk,
`value`
from {{ ref('mart_pmu_revenue') }} 