
create or replace table expediture_series_quintile_cat as
select *
from (
      select 'B0101M' type_of_quintile, 'All Consumer Units' type_of_quintile_txt
union select 'B0102M' type_of_quintile, 'Lowest 20 percent income quintile' type_of_quintile_txt
union select 'B0103M' type_of_quintile, 'Second 20 percent income quintile' type_of_quintile_txt
union select 'B0104M' type_of_quintile, 'Third 20 percent income quintile' type_of_quintile_txt
union select 'B0105M' type_of_quintile, 'Fourth 20 percent income quintile' type_of_quintile_txt
union select 'B0106M' type_of_quintile, 'Highest 20 percent income quintile' type_of_quintile_txt
union select 'B01A1M' type_of_quintile, 'Total complete income reporters' type_of_quintile_txt
union select 'B01A2M' type_of_quintile, 'Incomplete income reports' type_of_quintile_txt
)
;

create or replace table extended_match as
select
  mm.id
, mm.level
, mm.series_category
, mm."expenditure categories with spaces"
, mm.use_1997
, mm.level_0
, mm.level_1
, mm.level_2
, mm.level_3
, mm.level_4
, qc.type_of_quintile_txt
-- , mm.series_id_cx_all_consumer_units
, replace(mm.series_id_cx_all_consumer_units, 'B0101M', qc.type_of_quintile) series_id_cx
, qc.type_of_quintile
, mm.series_id_cu
, cx.series_title as cx_series_title
, cu.series_title as cu_series_title
, cx.begin_year as cx_begin_year
, cx.end_year as cx_end_year
, cu.begin_year as cu_begin_year
, cu.end_year as cu_end_year
, cx.begin_period as cx_begin_period
, cx.end_period as cx_end_period
, cu.begin_period as cu_begin_period
, cu.end_period as cu_end_period
from      my_matching_categories          as mm
cross join expediture_series_quintile_cat as qc
left join cu_series                       as cu on mm.series_id_cu = cu.series_id
left join cx_series                       as cx on replace(mm.series_id_cx_all_consumer_units, 'B0101M', qc.type_of_quintile) = cx.series_id
order by id
;


select *
from extended_match


create or replace table years as
select y::int as year
from generate_series(1997, 2021) as t(y);
;

create or replace table processing.basis as
select *
from extended_match
cross join years
;

create or replace table processing.flatfile as
select
  ba.*
, cx.seriesID    cx_seriesID
, cx.period      cx_period
, cx.periodName  cx_periodName
, cx.value       cx_value
, cx.footnotes   cx_footnotes
, cu.seriesID    cu_seriesID
, cu.period      cu_period
, cu.periodName  cu_periodName
, cu.value       cu_value
, cu.footnotes   cu_footnotes
, case when cu_value is not null and cx.value is not null then 1 else 0 end as is_valid_data
from      processing.basis   ba
left join main.series_import cx on ba.series_id_cx = cx.seriesID and ba.year = cx.period and cx.periodName = 'Annual'
left join main.series_import cu on ba.series_id_cu = cu.seriesID and ba.year = cu.period and cu.periodName = 'Annual'
where ba.series_category =  'expenditure'
and  ba.use_1997 = 1
;


create or replace table processing.check_file as
select
  ba.*
, cx.seriesID    cx_seriesID
, cx.period      cx_period
, cx.periodName  cx_periodName
, cx.value       cx_value
, cx.footnotes   cx_footnotes
, cu.seriesID    cu_seriesID
, cu.period      cu_period
, cu.periodName  cu_periodName
, cu.value       cu_value
, cu.footnotes   cu_footnotes
, case when cu_value is not null and cx.value is not null then 1 else 0 end as is_valid_data
from      processing.basis   ba
left join main.series_import cx on ba.series_id_cx = cx.seriesID and ba.year = cx.period and cx.periodName = 'Annual'
left join main.series_import cu on ba.series_id_cu = cu.seriesID and ba.year = cu.period and cu.periodName = 'Annual'
where ba.series_category =  'expenditure'
;

-- verify diff
select
*
, a.cx_value - b.cx_value
from (
select year,type_of_quintile_txt, sum(cx_value) cx_value
from processing.flatfile
where type_of_quintile_txt not in ('Incomplete income reports','Total complete income reporters')
group by
year,type_of_quintile_txt
order by year,type_of_quintile_txt
) a
full join
(select year,type_of_quintile_txt, sum(cx_value) cx_value
from processing.check_file
where type_of_quintile_txt not in ('Incomplete income reports','Total complete income reporters')
and  "Expenditure categories with spaces" = 'Average annual expenditures                        '
group by
year,type_of_quintile_txt
order by year,type_of_quintile_txt
) b on a.year = b.year and a.type_of_quintile_txt = b.type_of_quintile_txt
order by a.year desc,a.type_of_quintile_txt
;


select cx_value, *
from processing.check_file
where year = 2023
-- and use_1997 = 0
and type_of_quintile_txt = 'All Consumer Units'
order by ID
;


