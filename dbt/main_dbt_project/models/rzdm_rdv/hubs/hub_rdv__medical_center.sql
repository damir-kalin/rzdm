{{
  config(
    materialized='table',
    table_type='PRIMARY',
    keys=['medical_center_hk'],
    buckets=3,
    properties={
      'replication_num': '1'
    },
    pre_hook="DROP TABLE IF EXISTS {{ this }} FORCE"
  )
}}

select DISTINCT
	md5(`Наименование элемента`) as medical_center_hk,
	`Наименование элемента` as medical_center, 
	now() as  load_date,
    "Справочник" AS source_system 
    from stage.med_center_xlsx 