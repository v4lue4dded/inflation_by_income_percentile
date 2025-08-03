select
    *
from cx_series cx
where  cx.series_title like '% by Income Quintiles: All Consumer Units'
and cx.demographics_code = 'LB01' and cx.characteristics_code = '01'
and lower(cx.series_title) like '%household%textiles%'
and begin_year <= 1984

--
--
--
-- select
--     *
-- from cx_item_hierarchy
-- where item_text like '%Telephone%'