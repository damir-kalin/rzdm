{{
  config(
    materialized='table',
    table_type='PRIMARY',
    keys=['sales_channel_hk'],
    buckets=3,
    properties={
      'replication_num': '1'
    },
    pre_hook="DROP TABLE IF EXISTS {{ this }} FORCE"
  )
}}

select DISTINCT md5(`Короткое наименование`) as sales_channel_hk,
`Короткое наименование` as shortname,
    now() as  load_date,
    "Справочник" AS source_system 
    from stage.sales_channel_xlsx