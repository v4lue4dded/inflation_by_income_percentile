select substring(series_id,4,6) as cx_code
, 'ucc' as join_type
, *
from cx_series
where lower(series_title) like '%flour%'
and demographics_code = 'LB01'

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
where lower(series_title) like '%flour%'
;

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


create or replace temp table cx_basis_table as
select substring(series_id,4,6) as cx_code
, 'ucc' as join_type
, *
from cx_series
where demographics_code = 'LB01'
and item_code is null
;

select
  cx_code
, characteristics_code
, count(*) as freq
from cx_basis_table as x
group by
  cx_code
, characteristics_code
order by freq desc
;
select *
from cx_basis_table as x



select
  cx_code
, characteristics_code
, count(*) as freq
from cx_basis_table as ba
full join ce_cpi_concordance as co on ba.cx_code = co.UCC
group by
  cx_code
, characteristics_code
order by freq desc
;

select
  freq
, count(*) as freqfreq
from (
    select
      cx_code
    , characteristics_code
    , count(*) as freq
    from cx_basis_table as ba
    full join ce_cpi_concordance as co on ba.cx_code = co.UCC
    group by
      cx_code
    , characteristics_code
) x
group by
  freq
order by freqfreq desc
;



select *
from cx_basis_table as ba
full join ce_cpi_concordance as co on ba.cx_code = co.UCC
where cx_code = '580311'
and characteristics_code = '01'
