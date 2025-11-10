{{
  config(
    materialized='table',
    table_type='PRIMARY',
    keys=['road_hk'],
    buckets=3,
	comment='Хаб для дорог',
    properties={
      'replication_num': '1'
    },
    pre_hook="DROP TABLE IF EXISTS {{ this }} FORCE"
  )
}}


SELECT DISTINCT
    md5(`Наименование элемента`) as road_hk,
	`Наименование элемента` as fullname,
    now() as  load_date,
    "Справочник" AS source_system
from stage.road_xlsx  