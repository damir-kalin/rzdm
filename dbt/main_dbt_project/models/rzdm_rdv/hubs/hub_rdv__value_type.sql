{{
  config(
    materialized='table',
    table_type='PRIMARY',
    keys=['value_type_hk'],
    buckets=3,
    properties={
      'replication_num': '1'
    },
    pre_hook="DROP TABLE IF EXISTS {{ this }} FORCE"
  )
}}

SELECT 
    md5(Наименование) AS value_type_hk,
	Наименование as fullname,
    now() AS load_date,
    "База выручки" AS source_system
FROM stage.value_type_xlsx