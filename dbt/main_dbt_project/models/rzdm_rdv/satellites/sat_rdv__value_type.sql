{{
  config(
    materialized='incremental',
    table_type='PRIMARY',
    keys=['value_type_hk','load_date'],
    buckets=3,
    properties={
      'replication_num': '1'
    },
    partition_type='Expr')
}}

SELECT 
    md5(Наименование) AS value_type_hk,
	now() AS load_date,
	Наименование as fullname,
    "Справочник" AS source_system
FROM stage.value_type_xlsx 