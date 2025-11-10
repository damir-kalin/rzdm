{{
  config(
    materialized='view')
}}

SELECT 
value_type_hk,
fullname,
load_date,
source_system
FROM main_rdv.hub_rdv__value_type 