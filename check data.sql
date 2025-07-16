select substring(series_id,4,6) as ucc_code, *
from cx_series
where lower(series_title) like '%flour%'
and demographics_code = 'LB01'


select substring(series_id,11,6) as eli_code, *
from cu_series
where lower(series_title) like '%flour%'

select distinct  footnotes
from series_import


select *
from series_import
where footnotes = 'Prior to 2005 this item was titled, ''Television, radios, sound equipment''.'



select *
from      cu_series cu
inner join cx_series cx on cu.item_code = cx.item_code


select *
from      cu_series cu
inner join cx_series cx on cu.item_code = cx.item_code


