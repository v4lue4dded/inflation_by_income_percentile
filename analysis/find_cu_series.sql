select
    *
from cu_item_hierarchy
-- where sort_sequence >245
-- where level_0 = 'Transportation'
order by sort_sequence

select *
from cu_series
where series_title like  '% in U.S. city average, all urban consumers, seasonally adjusted'
where item_code like '%SEED%'
-- and(
--       lower(series_title) like '%phone%'
--     )
-- and begin_year <= 1984
order by  series_id

