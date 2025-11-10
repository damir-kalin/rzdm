{{
  config(
    materialized='view')
}}

SELECT 
sales_channel_hk,load_date,shortname,id,fullname,source_system
FROM main_rdv.sat_rdv__sales_channel
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY sales_channel_hk
    ORDER BY load_date DESC
) = 1