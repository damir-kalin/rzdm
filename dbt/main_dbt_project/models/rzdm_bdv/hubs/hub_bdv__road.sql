{{
  config(
    materialized='view')
}}


SELECT 
road_hk,
fullname,
load_date,
source_system
from main_rdv.hub_rdv__road 