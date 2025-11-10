{{
  config(
    materialized='table',
    table_type='PRIMARY',
    keys=['data_type_hk'],
    buckets=3,
    properties={
      'replication_num': '1'
    },
    pre_hook="DROP TABLE IF EXISTS {{ this }} FORCE"
  )
}}

select 
md5(`наименование элемента`) as data_type_hk, 
`наименование элемента` as fullname, 
now() as  load_date,
"Справочник" AS source_system  
from stage.data_type_xlsx 