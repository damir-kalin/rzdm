{{
  config(
    materialized='incremental',
    table_type='PRIMARY',
    keys=['road_hk','load_date'],
    buckets=3,
    properties={
      'replication_num': '1'
    },
    partition_type='Expr')
}}

SELECT DISTINCT
    md5(`Наименование элемента`) as road_hk,
	now() as  load_date,
	`Наименование элемента` as fullname,
	Индекс as id,
	`Код элемента справочника` as code,
	Комментарии as `comment`,
	`Порядок сортировки` as sort_type,
    "Справочник" AS source_system
from stage.road_xlsx  