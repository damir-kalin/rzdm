{{
  config(
    materialized='view')
}}

SELECT 
discreteness_hk,load_date,fullname,source_system
FROM main_rdv.sat_rdv__discreteness
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY discreteness_hk
    ORDER BY load_date DESC
) = 1