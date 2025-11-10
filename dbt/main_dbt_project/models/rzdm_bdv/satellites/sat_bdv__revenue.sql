{{
  config(
    materialized='table',
    table_type='PRIMARY',
    keys=['revenue_hk'],
    buckets=3,
    properties={
      'replication_num': '1'
    }
  )
}}

with source_data as (      
select 
h_rev.`year`,
h_rev.`month`,
sum(s_rev.fact__rub)as `value`, 
l_road_rev.road_hk,
l_med_rev.medical_center_hk,
s_vtype.value_type_hk,
s_unit.unit_hk,
s_disc.discreteness_hk,
s_met.metric_hk,
s_dtype.data_type_hk 
from main_rdv.hub_rdv__revenue h_rev
inner join main_rdv.sat_rdv__revenue s_rev on h_rev.revenue_hk = s_rev.revenue_hk  
-- связь выручки и дороги
inner join main_rdv.link_rdv__road_revenue l_road_rev on h_rev.revenue_hk = l_road_rev.revenue_hk 
-- связь выручки и мед центра
inner join main_rdv.link_rdv__medical_center_revenue l_med_rev on h_rev.revenue_hk = l_med_rev.revenue_hk
-- связь выручки и канала продаж
inner join main_rdv.link_rdv__sales_channel_revenue l_sale_rev on h_rev.revenue_hk = l_sale_rev.revenue_hk
-- связь выручки и дискретности
inner join main_rdv.link_rdv__discreteness_revenue l_disc_rev on h_rev.revenue_hk = l_disc_rev.revenue_hk
inner join main_rdv.sat_rdv__discreteness s_disc on s_disc.fullname = 'месяц' and s_disc.load_date = (select max(load_date) from main_rdv.sat_rdv__discreteness)
-- связь выручки и единиц измерения
inner join main_rdv.link_rdv__unit_revenue l_unit_rev on h_rev.revenue_hk = l_unit_rev.revenue_hk
inner join main_rdv.sat_rdv__unit s_unit on s_unit.unit_hk = l_unit_rev.unit_hk  and s_unit.load_date = (select max(load_date) from main_rdv.sat_rdv__unit)
-- связь выручки и типов значения
inner join main_rdv.link_rdv__value_type_revenue l_vtype_rev on h_rev.revenue_hk = l_vtype_rev.revenue_hk
inner join main_rdv.sat_rdv__value_type s_vtype on s_vtype.fullname = 'нарастающий итог по месяцам с начала года' and s_vtype.load_date = (select max(load_date) from main_rdv.sat_rdv__value_type)
-- связь выручки и секторов продаж
inner join main_rdv.link_rdv__sales_sector_revenue l_sale_sec_rev on h_rev.revenue_hk = l_sale_sec_rev.revenue_hk
inner join main_rdv.sat_rdv__sales_sector s_sale_sec on s_sale_sec.sales_sector_hk = l_sale_sec_rev.sales_sector_hk and s_sale_sec.load_date = (select max(load_date) from main_rdv.sat_rdv__sales_sector)\
-- связь выручки и метрики
inner join main_rdv.link_rdv__metric_revenue l_met_rev on h_rev.revenue_hk = l_met_rev.revenue_hk 
inner join main_rdv.sat_rdv__metric s_met on s_met.metric_hk = l_met_rev.metric_hk and s_met.load_date = (select max(load_date) from main_rdv.sat_rdv__metric)
-- связь выручки и типов данных
inner join main_rdv.link_rdv__data_type_revenue l_dtype_rev on h_rev.revenue_hk = l_dtype_rev.revenue_hk 
inner join main_rdv.sat_rdv__data_type s_dtype on s_dtype.fullname = 'расчетный' and s_dtype.load_date = (select max(load_date) from main_rdv.sat_rdv__data_type)
where s_sale_sec.shortname = 'ПМУ'
group by `year`, `month`, road_hk, l_med_rev.medical_center_hk, s_vtype.value_type_hk, s_unit.unit_hk ,s_disc.discreteness_hk,s_met.metric_hk,s_dtype.data_type_hk 

UNION ALL 

select 
h_rev.`year`,
null as `month`,
sum(s_rev.fact__rub)as `value`, 
l_road_rev.road_hk,
l_med_rev.medical_center_hk,
s_vtype.value_type_hk,
s_unit.unit_hk,
s_disc.discreteness_hk,
s_met.metric_hk,
s_dtype.data_type_hk  
from main_rdv.hub_rdv__revenue h_rev
inner join main_rdv.sat_rdv__revenue s_rev on h_rev.revenue_hk = s_rev.revenue_hk  
-- связь выручки и дороги
inner join main_rdv.link_rdv__road_revenue l_road_rev on h_rev.revenue_hk = l_road_rev.revenue_hk 
-- связь выручки и мед центра
inner join main_rdv.link_rdv__medical_center_revenue l_med_rev on h_rev.revenue_hk = l_med_rev.revenue_hk
-- связь выручки и канала продаж
inner join main_rdv.link_rdv__sales_channel_revenue l_sale_rev on h_rev.revenue_hk = l_sale_rev.revenue_hk
-- связь выручки и дискретности
inner join main_rdv.link_rdv__discreteness_revenue l_disc_rev on h_rev.revenue_hk = l_disc_rev.revenue_hk
inner join main_rdv.sat_rdv__discreteness s_disc on s_disc.fullname = 'год' and s_disc.load_date = (select max(load_date) from main_rdv.sat_rdv__discreteness)
-- связь выручки и единиц измерения
inner join main_rdv.link_rdv__unit_revenue l_unit_rev on h_rev.revenue_hk = l_unit_rev.revenue_hk
inner join main_rdv.sat_rdv__unit s_unit on s_unit.unit_hk = l_unit_rev.unit_hk and s_unit.load_date = (select max(load_date) from main_rdv.sat_rdv__unit)
-- связь выручки и типов значения
inner join main_rdv.link_rdv__value_type_revenue l_vtype_rev on h_rev.revenue_hk = l_vtype_rev.revenue_hk
inner join main_rdv.sat_rdv__value_type s_vtype on s_vtype.fullname = 'итог' and s_vtype.load_date = (select max(load_date) from main_rdv.sat_rdv__value_type)
-- связь выручки и секторов продаж
inner join main_rdv.link_rdv__sales_sector_revenue l_sale_sec_rev on h_rev.revenue_hk = l_sale_sec_rev.revenue_hk
inner join main_rdv.sat_rdv__sales_sector s_sale_sec on s_sale_sec.sales_sector_hk = l_sale_sec_rev.sales_sector_hk and s_sale_sec.load_date = (select max(load_date) from main_rdv.sat_rdv__sales_sector)
-- связь выручки и метрики
inner join main_rdv.link_rdv__metric_revenue l_met_rev on h_rev.revenue_hk = l_met_rev.revenue_hk 
inner join main_rdv.sat_rdv__metric s_met on s_met.metric_hk = l_met_rev.metric_hk and s_met.load_date = (select max(load_date) from main_rdv.sat_rdv__metric)
-- связь выручки и типов данных
inner join main_rdv.link_rdv__data_type_revenue l_dtype_rev on h_rev.revenue_hk = l_dtype_rev.revenue_hk 
inner join main_rdv.sat_rdv__data_type s_dtype on s_dtype.fullname = 'расчетный' and s_dtype.load_date = (select max(load_date) from main_rdv.sat_rdv__data_type)
where s_sale_sec.shortname = 'ПМУ' 
group by h_rev.`year`, road_hk, l_med_rev.medical_center_hk, s_vtype.value_type_hk, s_unit.unit_hk, s_disc.discreteness_hk,s_met.metric_hk,s_dtype.data_type_hk   
)
select md5(CONCAT_WS('|',`year`, `month`,road_hk, medical_center_hk,value_type_hk,unit_hk,discreteness_hk,metric_hk,data_type_hk)) as revenue_hk, value, now() as load_date, 'База выручки' as source_system from source_data