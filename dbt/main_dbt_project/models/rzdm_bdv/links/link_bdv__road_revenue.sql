{{
  config(
    materialized='table',
    table_type='PRIMARY',
    keys=['link_road_revenue_hk'],
    buckets=3,
    properties={
      'replication_num': '1'
    }
  )
}}

with hub_keys as (
    SELECT DISTINCT
        h_road.road_hk,
        h_rev.revenue_hk
    FROM main_bdv.hub_bdv__revenue h_rev
    INNER JOIN main_bdv.hub_bdv__road h_road
        ON h_road.road_hk = h_rev.road_hk 
)
        SELECT DISTINCT
    md5(CONCAT_WS(road_hk,revenue_hk)) AS link_road_revenue_hk,
    road_hk,
    revenue_hk,
    now() AS load_date,
    "База выручки" AS source_system
FROM hub_keys