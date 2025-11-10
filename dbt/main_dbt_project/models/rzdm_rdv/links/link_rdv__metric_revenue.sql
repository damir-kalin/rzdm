{{
  config(
    materialized='table',
    table_type='PRIMARY',
    keys=['link_metric_revenue_hk'],
    buckets=3,
    properties={
      'replication_num': '1'
    }
  )
}}

with hub_keys AS (
    SELECT DISTINCT
        h_met.metric_hk ,
        h_rev.revenue_hk
    FROM stage.db_revenue_xlsx s
    left JOIN main_rdv.hub_rdv__metric h_met
        ON h_met.fullname = 'Факт'
    INNER JOIN main_rdv.hub_rdv__revenue h_rev  
        ON h_rev.revenue_hk = h_rev.revenue_hk
)

SELECT DISTINCT
    md5(CONCAT_WS(metric_hk,revenue_hk)) AS link_metric_revenue_hk,
    metric_hk,
    revenue_hk,
    now() AS load_date,
    "База выручки" AS source_system
FROM hub_keys
