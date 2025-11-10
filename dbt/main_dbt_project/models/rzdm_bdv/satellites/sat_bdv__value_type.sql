{{
  config(
    materialized='view')
}}

SELECT 
value_type_hk,load_date,fullname,source_system
FROM main_rdv.sat_rdv__value_type
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY value_type_hk
    ORDER BY load_date DESC
) = 1