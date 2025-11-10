{{
  config(
    materialized='view')
}}

SELECT 
unit_hk,load_date,fullname,source_system
FROM main_rdv.sat_rdv__unit
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY unit_hk
    ORDER BY load_date DESC
) = 1