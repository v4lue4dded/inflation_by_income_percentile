select
    *
from cx_series cx
where  cx.series_title like '% by Income Quintiles: All Consumer Units'
and cx.demographics_code = 'LB01' and cx.characteristics_code = '01'
and lower(cx.series_title) like '%gift%'
and begin_year <= 1984


select
    *
from cx_series cx
where cx.series_id = 'CXU720310LB0201M'


select
    *
from cx_series cx
where  cx.series_title like '% by Income Quintiles: All Consumer Units'
and  lower(cx.series_title) like '%gift%'

--
--
--
-- select
--     *
-- from cx_item_hierarchy
-- where item_text like '%Telephone%'