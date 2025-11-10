{{
  config(
    materialized='table',
    table_type='PRIMARY',
    keys=['unit_hk'],
    buckets=3,
    properties={
      'replication_num': '1'
    },
    pre_hook="DROP TABLE IF EXISTS {{ this }} FORCE"
  )
}}

select md5(`Наименование элемента`) as unit_hk, 
`Наименование элемента` as fullname,
now() as  load_date,
    "База выручки" AS source_system 
from stage.unit_xlsx