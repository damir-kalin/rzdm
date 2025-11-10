{{
  config(
    materialized='view')
}}

SELECT 
medical_center_hk,load_date,fullname,id,code,`comment`,macroregion,railway_polygon,sort_type,region,financial_structure,source_system
FROM main_rdv.sat_rdv__medical_center
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY medical_center_hk
    ORDER BY load_date DESC
) = 1