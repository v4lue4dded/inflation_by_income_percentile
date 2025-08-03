
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

create or replace table extended_match as
select
  mm.id
, mm.level
, mm."expenditure cateogories with spaces"
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


select
  cx_begin_year
, count(*) as freq
from extended_match as x
group by
  cx_begin_year
order by freq desc
;


select
  cu_begin_year
, count(*) as freq
from extended_match as x
group by
  cu_begin_year
order by freq desc
;


select
  cu_begin_year
, level
, count(*) as freq
from extended_match as x
group by
  cu_begin_year
, level
order by freq desc
;


select *
from extended_match as x
where cx_begin_year = 2010



select *
from extended_match as x
where cu_begin_year = 2009
