{{
  config(
    materialized='table',
    table_type='PRIMARY',
    keys=['link_discreteness_revenue_hk'],
    buckets=3,
    properties={
      'replication_num': '1'
    }
  )
}}

with hub_keys AS (
    SELECT DISTINCT
        h_disc.discreteness_hk,
        h_rev.revenue_hk
    FROM stage.db_revenue_xlsx s
    INNER JOIN main_rdv.hub_rdv__discreteness h_disc
        ON h_disc.fullname  = 'месяц'
    INNER JOIN main_rdv.hub_rdv__revenue h_rev  
        ON h_rev.revenue_hk  = h_rev.revenue_hk    
)

SELECT DISTINCT
    md5(CONCAT_WS(discreteness_hk,revenue_hk)) AS link_discreteness_revenue_hk,
    discreteness_hk,
    revenue_hk,
    now() AS load_date,
    "База выручки" AS source_system
FROM hub_keys
