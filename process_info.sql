
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

select
  mm.id
, mm.level
, mm."expenditure cateogories with spaces"
, mm."expenditure cateogories"
, mm.check_name
, mm.series_id_cx_all_consumer_units
, replace(mm.series_id_cx_all_consumer_units, 'B0101M', qc.series_ending) series_id_cx
, mm.series_id_cu
, mm.series_title
, mm.questionable_match
, mm.comment
from      my_matching_categories          mm
cross join expediture_series_quintile_cat qc
order by id


select *
from cu_series
where series_id like '%SA0'

select *
from cx_series
where series_id like 'CXUTOTALEXPL%'



select *
from cx_series
where series_title like 'Cereal%Quin%'