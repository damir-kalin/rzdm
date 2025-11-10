{{
  config(
    materialized='view')
}}

SELECT 
road_hk,load_date,fullname,id,code,`comment`,sort_type,source_system
FROM main_rdv.sat_rdv__road
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY road_hk
    ORDER BY load_date DESC
) = 1