{{
  config(
    materialized='incremental',
    table_type='PRIMARY',
    keys=['sales_sector_hk','load_date'],
    buckets=3,
    properties={
      'replication_num': '1'
    },
    partition_type='Expr')
}}

 select 
 md5(`Сектор`) as sales_sector_hk,
 now() as  load_date,
 `Сектор` as shortname,
 ROW_NUMBER() OVER (ORDER BY `Сектор`) as id,
 `Сектор` as fullname,
  "База выручки" AS source_system 
 from stage.db_revenue_xlsx
 group by `Сектор` 