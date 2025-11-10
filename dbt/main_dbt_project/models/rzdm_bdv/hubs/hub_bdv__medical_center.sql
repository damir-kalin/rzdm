{{
  config(
    materialized='view')
}}

select
medical_center_hk,
medical_center, 
load_date,
source_system 
from main_rdv.hub_rdv__medical_center