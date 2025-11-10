{{
  config(
    materialized='incremental',
    table_type='PRIMARY',
    keys=['discreteness_hk','load_date'],
    buckets=3,
    properties={
      'replication_num': '1'
    },
    partition_type='Expr')
}}

select md5(наименование) as discreteness_hk, 
now() as load_date,
наименование as fullname,
"Справочник" AS source_system 
from stage.discreteness_xlsx