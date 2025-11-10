{{
  config(
    materialized='table',
    table_type='PRIMARY',
    keys=['link_value_type_revenue_hk'],
    buckets=3,
    properties={
      'replication_num': '1'
    }
  )
}}

with hub_keys AS (
    SELECT DISTINCT
        h_v_type.value_type_hk ,
        h_rev.revenue_hk
    FROM stage.db_revenue_xlsx s
    inner JOIN main_rdv.hub_rdv__value_type h_v_type
        ON h_v_type.fullname = 'нарастающий итог по суткам с начала месяца'
    INNER JOIN main_rdv.hub_rdv__revenue h_rev  
        ON h_rev.revenue_hk = md5(CONCAT_WS('|',s.`ЧУЗ`,cast(CONCAT(s.Год, '-',LPAD(s.Мес,2,'0'),'-01')as date),s.Канал))     
)

SELECT DISTINCT
    md5(CONCAT_WS(value_type_hk,revenue_hk)) AS link_value_type_revenue_hk,
    value_type_hk,
    revenue_hk,
    now() AS load_date,
    "База выручки" AS source_system
FROM hub_keys
