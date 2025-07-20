create or replace temp table cx_basis_table as
select substring(se.series_id,4,6) as cx_code
, 'ucc' as join_type
, *
from      cx_series         as se
left join cx_item_hierarchy as ih on se.item_code = ih.item_code
where se.demographics_code = 'LB01'
and   ih.display_level <= 2
;

create or replace temp table cu_basis_table as
select
  substring(se.series_id || '1', 11, 5) as cu_code_prep,
  case
    when cu_code_prep ~ '^[0-9]+$' then cu_code_prep || '0'
    else cu_code_prep
  end as cu_code,
  case
    when cu_code_prep ~ '^[0-9]+$' then 'ucc'
    else 'eli'
  end as join_type,
  *
from      cu_series         as se
left join cu_item_hierarchy as ih on se.item_code = ih.item_code
where ih.display_level <= 2
and se.area_code = '0000'
;

create or replace table processing.best_raw_match as
select
  cx.series_id     as cx_series_id
, cx.series_title  as cx_series_title
, cu.series_id     as cu_series_id
, cu.series_title  as cu_series_title
, cx.cx_code
, cu.cu_code
, cu.join_type
, co.*
, cx.*
, cu.*
from      cx_basis_table     as cx
full join ce_cpi_concordance as co on cx.cx_code = co.UCC
full join cu_basis_table     as cu on
    case
      when cu.join_type = 'ucc' then cx.cx_code = cu.cu_code
      when cu.join_type = 'eli' then co.ELI = cu.cu_code
    end
where cx.series_id is not null
or    cu.series_id is not null
;

select *
from processing.best_raw_match
where cu_series_id is null
or    cx_series_id is null

order by random()


select *
from cx_basis_table

select
  cx_series_id
, count(*) as freq
from processing.best_raw_match as x
group by
  cx_series_id
order by freq desc
;


select
from processing.best_raw_match



select *
from processing.best_raw_match
where lower(cu_series_title) like '%cookies%'
or    lower(cx_series_title) like '%cookies%'
