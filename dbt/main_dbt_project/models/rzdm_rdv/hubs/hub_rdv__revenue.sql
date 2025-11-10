{{
  config(
    materialized='table',
    table_type='PRIMARY',
    keys=['revenue_hk'],
    buckets=3,
    properties={
      'replication_num': '1'
    },
    pre_hook="DROP TABLE IF EXISTS {{ this }} FORCE"
  )
}}



select md5(CONCAT_WS('|',`ЧУЗ`,period,Канал)) as revenue_hk, 
CAST(Год as int) as `year`, cast(Мес as int) as `month`, Канал as sales_channel,`ЧУЗ` as medical_center, cast(CONCAT(Год, '-',LPAD(Мес,2,'0'),'-01')as date) as `period`,
now() as  load_date,
    "База выручки" AS source_system 
from stage.db_revenue_xlsx