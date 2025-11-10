{{
  config(
    materialized='incremental',
    table_type='PRIMARY',
    keys=['data_type_hk','load_date'],
    buckets=3,
    properties={
      'replication_num': '1'
    },
    partition_type='Expr')
}}

select 
md5(`наименование элемента`) as data_type_hk, 
now() as  load_date,
`наименование элемента` as fullname, 
"Справочник" AS source_system  
from stage.data_type_xlsx