
from cu_series



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


