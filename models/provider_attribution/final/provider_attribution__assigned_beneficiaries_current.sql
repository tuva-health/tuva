{{ config(
     enabled = var('tuva_provider_attribution', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
   )
}}

with claim_bounds as (
  select max(claim_end_date) as max_claim_end_date
  from {{ ref('provider_attribution__int_primary_care_claims') }}
)

{% set override_as_of_date = var('provider_attribution_as_of_date', none) %}

, params as (
  select
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

, months_12 as (
  -- Build the last 12 calendar months (YYYYMM) ending at as_of_date
  select distinct
      c.year_month_int
    , c.first_day_of_month
    , c.last_day_of_month
  from {{ ref('provider_attribution__stg_reference_data__calendar') }} as c
  cross join params as p
  where c.full_date >= cast({{ dbt.dateadd(datepart='month', interval=-11, from_date_or_timestamp='p.as_of_date') }} as date)
    and c.full_date <= p.as_of_date
)

, months_24 as (
  -- Build the last 24 calendar months ending at as_of_date (for fallback bounds)
  select distinct
      c.year_month_int
    , c.first_day_of_month
    , c.last_day_of_month
  from {{ ref('provider_attribution__stg_reference_data__calendar') }} as c
  cross join params as p
  where c.full_date >= cast({{ dbt.dateadd(datepart='month', interval=-23, from_date_or_timestamp='p.as_of_date') }} as date)
    and c.full_date <= p.as_of_date
)

, lookback_24 as (
  select min(first_day_of_month) as lookback_start_date_24
  from months_24
)

, lookback_bounds as (
  select
      l24.lookback_start_date_24
  from lookback_24 as l24
)

, eligible as (
  select distinct mm.person_id
  from {{ ref('provider_attribution__stg_core__member_months') }} as mm
  inner join months_12 as m
    on mm.year_month = cast(m.year_month_int as {{ dbt.type_string() }})
)

, assigned as (
  select
      pr.person_id
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
  cross join params as p
  inner join eligible as e
    on pr.person_id = e.person_id
  where pr.scope = 'current'
    and pr.as_of_date = p.as_of_date
    and pr.ranking = 1
)

, missing as (
  select
      e.person_id
  from eligible as e
  left outer join assigned as a
    on e.person_id = a.person_id
  where a.person_id is null
)

, fallback as (
  select
      m.person_id
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
    , {{ concat_custom(["'current|'", "replace(cast(p.as_of_date as " ~ dbt.type_string() ~ "),'-','')", "'|'", "m.person_id"]) }} as attribution_key
  from missing as m
  cross join params as p
  cross join lookback_bounds as lb
)

select
    person_id
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
