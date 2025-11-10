{{
  config(
    materialized='table',
    table_type='PRIMARY',
    keys=['discreteness_hk'],
    buckets=3,
    properties={
      'replication_num': '1'
    },
    pre_hook="DROP TABLE IF EXISTS {{ this }} FORCE"
  )
}}

select md5(наименование) as discreteness_hk, 
наименование as fullname,
now() as  load_date,
    "Справочник" AS source_system 
from stage.discreteness_xlsx