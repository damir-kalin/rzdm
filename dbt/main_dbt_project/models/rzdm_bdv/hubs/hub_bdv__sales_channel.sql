{{
  config(
    materialized='view')
}}

select sales_channel_hk,
shortname,
load_date,
source_system 
from main_rdv.hub_rdv__sales_channel