{{
  config(
    materialized='table',
    table_type='PRIMARY',
    keys=['sales_sector_hk'],
    buckets=3,
    properties={
      'replication_num': '1'
    },
    pre_hook="DROP TABLE IF EXISTS {{ this }} FORCE"
  )
}}

select md5(`Сектор`) as sales_sector_hk, 
`Сектор` as shortname,
now() as  load_date,
    "База выручки" AS source_system 
from stage.db_revenue_xlsx
group by `Сектор`