{{
  config(
    materialized='incremental',
    table_type='PRIMARY',
    keys=['sales_channel_hk','load_date'],
    buckets=3,
    properties={
      'replication_num': '1'
    },
    partition_type='Expr')
}}

select DISTINCT md5(`Короткое наименование`) as sales_channel_hk,
now() as  load_date,
`Короткое наименование` as shortname,
Индекс as id,
Наименование as fullname,
"Справочник" AS source_system 
from stage.sales_channel_xlsx