{{
  config(
    materialized='table',
    table_type='PRIMARY',
    keys=['link_medical_center_revenue_hk'],
    buckets=3,
    properties={
      'replication_num': '1'
    }
  )
}}

with hub_keys AS (
    SELECT DISTINCT
        h_med.medical_center_hk ,
        h_rev.revenue_hk
    FROM stage.db_revenue_xlsx s
    INNER JOIN main_rdv.hub_rdv__medical_center h_med
        ON h_med.medical_center = s.ЧУЗ 
    INNER JOIN main_rdv.hub_rdv__revenue h_rev  
        ON h_rev.revenue_hk = md5(CONCAT_WS('|',s.`ЧУЗ`,cast(CONCAT(s.Год, '-',LPAD(s.Мес,2,'0'),'-01')as date),s.Канал))  
)

SELECT DISTINCT
    md5(CONCAT_WS(medical_center_hk,revenue_hk)) AS link_medical_center_revenue_hk,
    medical_center_hk,
    revenue_hk,
    now() AS load_date,
    "База выручки" AS source_system
FROM hub_keys
