{{
  config(
    materialized='view')
}}

SELECT 
metric_hk,load_date,fullname,source_system
FROM main_rdv.sat_rdv__metric
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY metric_hk
    ORDER BY load_date DESC
) = 1