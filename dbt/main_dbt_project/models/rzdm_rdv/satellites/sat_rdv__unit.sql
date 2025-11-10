{{
  config(
    materialized='incremental',
    table_type='PRIMARY',
    keys=['unit_hk','load_date'],
    buckets=3,
    properties={
      'replication_num': '1'
    },
    partition_type='Expr')
}}

select md5(`Наименование элемента`) as unit_hk, 
now() as  load_date,
`Наименование элемента` as fullname,
"База выручки" AS source_system 
from stage.unit_xlsx