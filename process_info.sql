

select
*
from my_matching_categories

select *
from cx_series
where series_id like 'CXUTOTALEXPL%'

      select 'B0101M' series_ending, 'All Consumer Units' series_ending_txt
union select 'B0102M' series_ending, 'Lowest 20 percent income quintile' series_ending_txt
union select 'B0103M' series_ending, 'Second 20 percent income quintile' series_ending_txt
union select 'B0104M' series_ending, 'Third 20 percent income quintile' series_ending_txt
union select 'B0105M' series_ending, 'Fourth 20 percent income quintile' series_ending_txt
union select 'B0106M' series_ending, 'Highest 20 percent income quintile' series_ending_txt
union select 'B01A1M' series_ending, 'Total complete income reporters' series_ending_txt
union select 'B01A2M' series_ending, 'Incomplete income reports' series_ending_txt


select *
from cx_series
where series_title like 'Cereal%Quin%'