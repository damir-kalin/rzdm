{{
  config(
    materialized='table',
    tags=['marts', 'fact']
  )
}}

select 
cast(CONCAT(h_rev.`year`, '-',LPAD(COALESCE(h_rev.`month`,'1'),2,'0'),'-01')as date) as `date`, 
h_rev.road_hk,
h_rev.medical_center_hk,
h_rev.value_type_hk,
h_rev.discreteness_hk,
s_unit.unit_hk,
h_rev.metric_hk,
h_rev.data_type_hk,
(s_rev.`value`/1000) as `value`
from {{ ref('hub_bdv__revenue') }} h_rev
-- подтягиваем рассчитанные показатели
inner join {{ ref('sat_bdv__revenue') }} s_rev on h_rev.revenue_hk = s_rev.revenue_hk 
-- меняем единицы измерения с рублей на тысячи рублей
inner join {{ ref('sat_bdv__unit') }} s_unit on s_unit.fullname = 'Тысяча рублей'
