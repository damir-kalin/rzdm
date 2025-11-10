{{
  config(
    materialized='view')
}}

SELECT 
data_type_hk,load_date,fullname,source_system
FROM main_rdv.sat_rdv__data_type
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY data_type_hk
    ORDER BY load_date DESC
) = 1