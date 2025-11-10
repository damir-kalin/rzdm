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

with hub_keys as (
    SELECT DISTINCT
        h_med.medical_center_hk,
        h_rev.revenue_hk
    FROM main_bdv.hub_bdv__revenue h_rev
    INNER JOIN main_bdv.hub_bdv__medical_center h_med
        ON h_med.medical_center_hk = h_rev.medical_center_hk 
)
        SELECT DISTINCT
    md5(CONCAT_WS(medical_center_hk,revenue_hk)) AS link_medical_center_revenue_hk,
    medical_center_hk,
    revenue_hk,
    now() AS load_date,
    "База выручки" AS source_system
FROM hub_keys