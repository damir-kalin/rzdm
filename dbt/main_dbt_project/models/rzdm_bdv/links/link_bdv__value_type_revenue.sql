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

with hub_keys as (
    SELECT DISTINCT
        h_vtype.value_type_hk,
        h_rev.revenue_hk
    FROM main_bdv.hub_bdv__revenue h_rev
    INNER JOIN main_bdv.hub_bdv__value_type h_vtype
        ON h_vtype.value_type_hk = h_rev.value_type_hk 
)
        SELECT DISTINCT
    md5(CONCAT_WS(value_type_hk,revenue_hk)) AS link_value_type_revenue_hk,
    value_type_hk,
    revenue_hk,
    now() AS load_date,
    "База выручки" AS source_system
FROM hub_keys