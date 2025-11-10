{{
  config(
    materialized='incremental',
    table_type='PRIMARY',
    keys=['medical_center_hk','load_date'],
    buckets=3,
    properties={
      'replication_num': '1'
    },
    partition_type='Expr')
}}

select DISTINCT
	md5(`Наименование элемента`) as medical_center_hk,
	now() as  load_date,
	`Наименование элемента` as fullname,
	Индекс as id,
	`Код элемента справочника` as code,
	Комментарии as `comment`,
	`Макрорегионы России` as macroregion,
	`Полигоны ЖД` as railway_polygon,
	`Порядок сортировки` as sort_type,
	`Регионы России` as region,
	`Финансовая структура` as financial_structure,
	"Справочник" AS source_system 
    from stage.med_center_xlsx 