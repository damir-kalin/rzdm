{{
  config(
    materialized='table',
    table_type='PRIMARY',
    keys=['link_data_type_revenue_hk'],
    buckets=3,
    properties={
      'replication_num': '1'
    }
  )
}}

with hub_keys AS (
    SELECT DISTINCT
        h_dtype.data_type_hk ,
        h_rev.revenue_hk
    FROM stage.db_revenue_xlsx s
    left JOIN main_rdv.hub_rdv__data_type h_dtype
        ON h_dtype.fullname = 'первичный'
    INNER JOIN main_rdv.hub_rdv__revenue h_rev  
        ON h_rev.revenue_hk = h_rev.revenue_hk 
)

SELECT DISTINCT
    md5(CONCAT_WS(data_type_hk,revenue_hk)) AS link_data_type_revenue_hk,
    data_type_hk,
    revenue_hk,
    now() AS load_date,
    "База выручки" AS source_system
FROM hub_keys
