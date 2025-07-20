
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


select
  display_level
, count(*) as freq
from cx_item as x
group by
  display_level
order by freq desc
;

select
  display_level
, count(*) as freq
from cu_item as x
group by
  display_level
order by freq desc
;

select
*
from cx_item_hierarchy as x
where display_level <=3
order by sort_sequence
;

select
*
from cx_item_hierarchy as x
where display_level <=4
order by sort_sequence
;



select
*
from cu_item_hierarchy as x
where display_level = 4
;


select
*
from cx_item as x
where lower(item_text) like '%egg%'
;

select
*
from cu_item as x
where lower(item_name) like '%egg%'
;


