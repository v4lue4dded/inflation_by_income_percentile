select substring(series_id,4,6) as cx_code
, 'ucc' as join_type
, *
from cx_series
where lower(series_title) like '%flour%'
and demographics_code = 'LB01'


select
  substring(series_id || '1', 11, 5) as cu_code_prep,
  case
    when cu_code_prep ~ '^[0-9]+$' then cu_code_prep || '0'
    else cu_code_prep
  end as cu_code,
  case
    when cu_code_prep ~ '^[0-9]+$' then 'ucc'
    else 'eli'
  end as join_type,
  *
from cu_series
where lower(series_title) like '%flour%'


select substring(series_id,4,6) as cx_code
, 'ucc' as join_type
, *
from cx_series
where (lower(series_title) like '%cake%'
or lower(series_title) like '%cookies%'    )
and demographics_code = 'LB01'
order by  cx_code
;

select
  substring(series_id || '1', 11, 5) as cu_code_prep,
  case
    when cu_code_prep ~ '^[0-9]+$' then cu_code_prep || '0'
    else cu_code_prep
  end as cu_code,
  case
    when cu_code_prep ~ '^[0-9]+$' then 'ucc'
    else 'eli'
  end as join_type,
  *
from cu_series
where lower(series_title) like '%cake%'
   or lower(series_title) like '%cookies%'
order by cu_code;

;



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


