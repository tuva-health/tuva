{{ config(
    enabled = var('pqi_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
) }}

with num as (
    select
        data_source
      , year_number
      , pqi_number
      , count(encounter_id) as num_count
    from {{ ref('ahrq_measures__pqi_num_long') }}
    group by
        data_source
      , year_number
      , pqi_number
)

, denom as (
    select
        data_source
      , year_number
      , pqi_number
      , count(person_id) as denom_count
    from {{ ref('ahrq_measures__pqi_denom_long') }} as d
    group by
        data_source
      , year_number
      , pqi_number
)

select
    d.data_source
  , d.year_number
  , d.pqi_number
  , d.denom_count
  , coalesce(num.num_count, 0) as num_count
  , coalesce(num.num_count, 0) / d.denom_count * 100000 as rate_per_100_thousand
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from denom as d
left outer join num
    on d.pqi_number = num.pqi_number
    and d.year_number = num.year_number
    and d.data_source = num.data_source
