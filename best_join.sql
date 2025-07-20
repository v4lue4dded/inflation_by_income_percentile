create or replace temp table cx_basis_table as
select substring(series_id,4,6) as cx_code
, 'ucc' as join_type
, *
from cx_series
where demographics_code = 'LB01'
and item_code is null
;

create or replace temp table cu_basis_table as
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
;


create or replace schema processing;

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
from      cx_basis_table     as cx
full join ce_cpi_concordance as co on cx.cx_code = co.UCC
full join cu_basis_table     as cu on
    case
      when cu.join_type = 'ucc' then cx.cx_code = cu.cu_code
      when cu.join_type = 'eli' then co.ELI = cu.cu_code
    end
;

select *
from processing.best_raw_match
where cu_series_id is null

order by random()


select *
from processing.best_raw_match
where lower(cu_series_title) like '%cookies%'
or    lower(cx_series_title) like '%cookies%'
