{{ config(
     enabled = (var('provider_attribution_enabled', False) and var('claims_enabled', False))
   )
}}

with claim_bounds as (
  select
      data_source
    , max(claim_end_date) as max_claim_end_date
  from {{ ref('provider_attribution__int_primary_care_claims') }}
  group by data_source
)

{% set override_as_of_date = var('provider_attribution_as_of_date', none) %}

, params as (
  select
    data_source,
    {% if override_as_of_date %}
      cast('{{ override_as_of_date }}' as date) as as_of_date
    {% else %}
      case
        when max_claim_end_date is not null
          and max_claim_end_date <= cast({{ dbt.current_timestamp() }} as date)
          then max_claim_end_date
        else cast({{ dbt.current_timestamp() }} as date)
      end as as_of_date
    {% endif %}
  from claim_bounds
)

, calendar_months as (
  select
      year_month_int
    , first_day_of_month
    , last_day_of_month
  from {{ ref('member_month__month_spine') }}
)

, months_12 as (
  -- Build the last 12 calendar months (YYYYMM) ending at as_of_date
  select distinct
      p.data_source
    , c.year_month_int
    , c.first_day_of_month
    , c.last_day_of_month
  from calendar_months as c
  cross join params as p
  where c.last_day_of_month >= cast({{ dbt.dateadd(datepart='month', interval=-11, from_date_or_timestamp='p.as_of_date') }} as date)
    and c.first_day_of_month <= p.as_of_date
)

, months_24 as (
  -- Build the last 24 calendar months ending at as_of_date (for fallback bounds)
  select distinct
      p.data_source
    , c.year_month_int
    , c.first_day_of_month
    , c.last_day_of_month
  from calendar_months as c
  cross join params as p
  where c.last_day_of_month >= cast({{ dbt.dateadd(datepart='month', interval=-23, from_date_or_timestamp='p.as_of_date') }} as date)
    and c.first_day_of_month <= p.as_of_date
)

, lookback_24 as (
  select
      data_source
    , min(first_day_of_month) as lookback_start_date_24
  from months_24
  group by data_source
)

, lookback_bounds as (
  select
      l24.data_source
    , l24.lookback_start_date_24
  from lookback_24 as l24
)

, eligible as (
  select distinct
      mm.person_id
    , mm.data_source
  from {{ ref('member_month__member_month') }} as mm
  inner join months_12 as m
    on mm.data_source = m.data_source
   and mm.year_month = cast(m.year_month_int as {{ dbt.type_string() }})
)

, assigned as (
  select
      pr.person_id
    , pr.data_source
    , pr.as_of_date
    , pr.provider_id
    , pr.provider_bucket
    , pr.prov_specialty
    , pr.step as assigned_step
    , pr.step_description
    , pr.allowed_amount
    , pr.visits
    , pr.lookback_start_date
    , pr.lookback_end_date
    , pr.attribution_key
  from {{ ref('provider_attribution__provider_ranking') }} as pr
  inner join params as p
    on pr.data_source = p.data_source
  inner join eligible as e
    on pr.person_id = e.person_id
   and pr.data_source = e.data_source
  where pr.scope = 'current'
    and pr.as_of_date = p.as_of_date
    and pr.ranking = 1
)

, missing as (
  select
      e.person_id
    , e.data_source
  from eligible as e
  left outer join assigned as a
    on e.person_id = a.person_id
   and e.data_source = a.data_source
  where a.person_id is null
)

, fallback as (
  select
      m.person_id
    , m.data_source
    , p.as_of_date
    , '9999999999' as provider_id
    , 'no_eligible_history' as provider_bucket
    , 'No assignable claims history' as prov_specialty
    , 0 as assigned_step
    , 'No assignable history' as step_description
    , cast(0 as {{ dbt.type_numeric() }}) as allowed_amount
    , 0 as visits
    , lb.lookback_start_date_24 as lookback_start_date
    , p.as_of_date as lookback_end_date
    , {{ concat_custom(["'current|'", "m.data_source", "'|'", "replace(cast(p.as_of_date as " ~ dbt.type_string() ~ "),'-','')", "'|'", "m.person_id"]) }} as attribution_key
  from missing as m
  inner join params as p
    on m.data_source = p.data_source
  inner join lookback_bounds as lb
    on m.data_source = lb.data_source
)

select
    person_id
  , data_source
  , as_of_date
  , provider_id
  , provider_bucket
  , prov_specialty
  , assigned_step
  , step_description
  , allowed_amount
  , visits
  , lookback_start_date
  , lookback_end_date
  , attribution_key
  , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from assigned

union all

select
    person_id
  , data_source
  , as_of_date
  , provider_id
  , provider_bucket
  , prov_specialty
  , assigned_step
  , step_description
  , allowed_amount
  , visits
  , lookback_start_date
  , lookback_end_date
  , attribution_key
  , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from fallback
