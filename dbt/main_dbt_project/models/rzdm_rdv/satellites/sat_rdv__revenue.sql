{{
  config(
    materialized='table',
    table_type='DUPLICATE',
    keys=['revenue_hk','load_date'],
    buckets=3,
    properties={
      'replication_num': '1'
    },
    partition_type='Expr')
}}

select 
md5(CONCAT_WS('|',`ЧУЗ`,cast(CONCAT(Год, '-',LPAD(Мес,2,'0'),'-01')as date),Канал)) AS revenue_hk,
now() as  load_date,
CAST(COALESCE(`План КДРО, руб.`, '0') AS DECIMAL(15,2)) AS plan_kdro__rub,
CAST(COALESCE(`План утвержденный, руб.`, '0') AS DECIMAL(15,2)) AS plan_approved__rub,
CAST(COALESCE(`Факт, руб.`, '0') AS DECIMAL(15,2)) AS fact__rub,
"База выручки" AS source_system 
from stage.db_revenue_xlsx
