
create or replace table expediture_series_quintile_cat as
select *
from (
      select 'B0101M' series_ending, 'All Consumer Units' series_ending_txt
union select 'B0102M' series_ending, 'Lowest 20 percent income quintile' series_ending_txt
union select 'B0103M' series_ending, 'Second 20 percent income quintile' series_ending_txt
union select 'B0104M' series_ending, 'Third 20 percent income quintile' series_ending_txt
union select 'B0105M' series_ending, 'Fourth 20 percent income quintile' series_ending_txt
union select 'B0106M' series_ending, 'Highest 20 percent income quintile' series_ending_txt
union select 'B01A1M' series_ending, 'Total complete income reporters' series_ending_txt
union select 'B01A2M' series_ending, 'Incomplete income reports' series_ending_txt
)
;

create or replace table extended_match as
select
  mm.id
, mm.level
, mm.series_category
, mm."expenditure cateogories with spaces"
, mm.level_0
, mm.level_1
, mm.level_2
, mm.level_3
, mm.level_4
-- , mm.series_id_cx_all_consumer_units
, replace(mm.series_id_cx_all_consumer_units, 'B0101M', qc.series_ending) series_id_cx
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
left join cx_series                       as cx on replace(mm.series_id_cx_all_consumer_units, 'B0101M', qc.series_ending) = cx.series_id
order by id
;

CREATE TABLE years AS
SELECT y::INT AS year
FROM generate_series(1984, 2023) AS t(y);
;

create table processing.basis as
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
;
