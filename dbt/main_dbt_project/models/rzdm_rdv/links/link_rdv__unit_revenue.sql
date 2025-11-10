{{
  config(
    materialized='table',
    table_type='PRIMARY',
    keys=['link_unit_revenue_hk'],
    buckets=3,
    properties={
      'replication_num': '1'
    }
  )
}}

with hub_keys AS (
    SELECT DISTINCT
        h_unit.unit_hk  ,
        h_rev.revenue_hk
    FROM stage.db_revenue_xlsx s
    left JOIN main_rdv.hub_rdv__unit h_unit
        ON h_unit.fullname = 'Рубль'
    INNER JOIN main_rdv.hub_rdv__revenue h_rev  
        ON h_rev.revenue_hk  = h_rev.revenue_hk  
)

SELECT DISTINCT
    md5(CONCAT_WS(unit_hk,revenue_hk)) AS link_unit_revenue_hk,
    unit_hk,
    revenue_hk,
    now() AS load_date,
    "База выручки" AS source_system
FROM hub_keys
