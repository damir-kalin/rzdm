{{
  config(
    materialized='table',
    table_type='PRIMARY',
    keys=['link_sales_channel_revenue_hk'],
    buckets=3,
    properties={
      'replication_num': '1'
    }
  )
}}

with src as (
    select distinct
        sales_channel_hk,
        revenue_hk
    from {{ ref('link_rdv__sales_channel_revenue') }}
)
select
    md5(CONCAT_WS('|', sales_channel_hk, revenue_hk)) as link_sales_channel_revenue_hk,
    sales_channel_hk,
    revenue_hk,
    now() as load_date,
    'База выручки' as source_system
from src

