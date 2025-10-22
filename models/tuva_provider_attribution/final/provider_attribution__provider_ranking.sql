{{ config(
     enabled = var('tuva_provider_attribution', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
   )
}}

-- Build a comprehensive provider ranking that shows all potential providers
-- a beneficiary could be attributed to, along with the earliest step each
-- provider qualifies for. Final assignment models can then pick rank = 1.

with calendar_months as (
  select distinct
      year_month_int
    , first_day_of_month
    , last_day_of_month
  from {{ ref('provider_attribution__stg_reference_data__calendar') }}
)

, claim_bounds as (
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

-- Rolling “current” window helpers
, months_12 as (
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
  select distinct
      c.year_month_int
    , c.first_day_of_month
    , c.last_day_of_month
  from {{ ref('provider_attribution__stg_reference_data__calendar') }} as c
  cross join params as p
  where c.full_date >= cast({{ dbt.dateadd(datepart='month', interval=-23, from_date_or_timestamp='p.as_of_date') }} as date)
    and c.full_date <= p.as_of_date
)

, lookback_12 as (
  select min(first_day_of_month) as lookback_start_date_12
  from months_12
)

, lookback_24 as (
  select min(first_day_of_month) as lookback_start_date_24
  from months_24
)

, lookback_bounds as (
  select
      l12.lookback_start_date_12
    , l24.lookback_start_date_24
  from lookback_12 as l12
  cross join lookback_24 as l24
)

, eligible_current as (
  -- Current-scope eligibility: beneficiaries with at least one member month
  -- within the 12-month window ending at as_of_date.
  select distinct mm.person_id
  from {{ ref('provider_attribution__stg_core__member_months') }} as mm
  inner join months_12 as m
    on mm.year_month = cast(m.year_month_int as {{ dbt.type_string() }})
)

, claims_12 as (
  select
      c.person_id
    , c.provider_id
    , c.provider_bucket
    , c.prov_specialty
    , c.encounter_id
    , c.claim_id
    , c.claim_year_month
    , c.claim_year_month_int
    , c.claim_end_date
    , c.allowed_amount
  from {{ ref('provider_attribution__int_primary_care_claims') }} as c
  inner join months_12 as m
    on c.claim_year_month_int = m.year_month_int
  cross join params as p
  where c.claim_end_date <= p.as_of_date
)

, claims_24 as (
  select
      c.person_id
    , c.provider_id
    , c.provider_bucket
    , c.prov_specialty
    , c.encounter_id
    , c.claim_id
    , c.claim_year_month
    , c.claim_year_month_int
    , c.claim_end_date
    , c.allowed_amount
  from {{ ref('provider_attribution__int_primary_care_claims') }} as c
  inner join months_24 as m
    on c.claim_year_month_int = m.year_month_int
  cross join params as p
  where c.claim_end_date <= p.as_of_date
)

, all_claim_month as (
  select
      mc.person_id
    , mc.claim_id
    , mc.claim_line_number
    , cast(cm.encounter_id as {{ dbt.type_string() }}) as encounter_id
    , mc.claim_start_date
    , mc.claim_end_date
    , cal.year_month_int as claim_year_month_int
    , cast(cal.year_month_int as {{ dbt.type_string() }}) as claim_year_month
    , coalesce(nullif(mc.allowed_amount, 0), mc.paid_amount, 0) as allowed_amount
    , cast(mc.rendering_npi as {{ dbt.type_string() }}) as provider_id
  from {{ ref('provider_attribution__stg_input_layer__medical_claim') }} as mc
  left outer join {{ ref('provider_attribution__stg_core__claims_medical_claim') }} as cm
    on mc.claim_id = cm.claim_id
   and mc.claim_line_number = cm.claim_line_number
   and mc.data_source = cm.data_source
  left outer join {{ ref('provider_attribution__stg_reference_data__calendar') }} as cal
    on cast(mc.claim_start_date as date) = cal.full_date
)

, eligible_all_claims as (
  select ac.*
  from all_claim_month as ac
  inner join {{ ref('provider_attribution__stg_core__member_months') }} as mm
    on ac.person_id = mm.person_id
   and ac.claim_year_month = mm.year_month
)

, all_rendering_claims as (
  select
      e.person_id
    , e.provider_id
    , e.encounter_id
    , e.claim_id
    , e.claim_year_month
    , e.claim_year_month_int
    , e.claim_end_date
    , e.allowed_amount
    , coalesce(pc.provider_bucket, 'other_individual') as provider_bucket
    , coalesce(pc.prov_specialty, sp.primary_specialty_description) as prov_specialty
  from eligible_all_claims as e
  inner join {{ ref('provider_attribution__stg_terminology__provider') }} as sp
    on cast(e.provider_id as {{ dbt.type_string() }}) = cast(sp.npi as {{ dbt.type_string() }})
   and lower(trim(sp.entity_type_description)) = 'individual'
  left outer join {{ ref('provider_attribution__provider_classification') }} as pc
    on e.provider_id = pc.provider_id
)

-- Build all potential providers (no bene-level gating across steps), then
-- collapse to the earliest qualifying step per person/provider.
, current_all_steps as (
  select person_id, provider_id, provider_bucket, prov_specialty, 1 as step
         , sum(allowed_amount) as allowed_amount
         , count(distinct encounter_id) as visits
  from claims_12
  where provider_id is not null and provider_bucket in ('pcp', 'npp')
  group by person_id, provider_id, provider_bucket, prov_specialty

  union all
  select person_id, provider_id, provider_bucket, prov_specialty, 2 as step
         , sum(allowed_amount) as allowed_amount
         , count(distinct encounter_id) as visits
  from claims_12
  where provider_id is not null and provider_bucket = 'specialist'
  group by person_id, provider_id, provider_bucket, prov_specialty

  union all
  select person_id, provider_id, provider_bucket, prov_specialty, 3 as step
         , sum(allowed_amount) as allowed_amount
         , count(distinct encounter_id) as visits
  from claims_24
  where provider_id is not null and provider_bucket in ('pcp', 'npp')
  group by person_id, provider_id, provider_bucket, prov_specialty

  union all
  select person_id, provider_id, provider_bucket, prov_specialty, 4 as step
         , sum(allowed_amount) as allowed_amount
         , count(distinct encounter_id) as visits
  from claims_24
  where provider_id is not null
  group by person_id, provider_id, provider_bucket, prov_specialty

  union all
  select arc.person_id, arc.provider_id, coalesce(arc.provider_bucket, 'unknown') as provider_bucket
         , arc.prov_specialty, 5 as step
         , sum(arc.allowed_amount) as allowed_amount
         , count(distinct arc.encounter_id) as visits
  from all_rendering_claims as arc
  inner join months_24 as m on arc.claim_year_month_int = m.year_month_int
  cross join params as p
  where arc.provider_id is not null and arc.claim_end_date <= p.as_of_date
  group by arc.person_id, arc.provider_id, coalesce(arc.provider_bucket, 'unknown'), arc.prov_specialty
)

, current_unique as (
  select *
  from (
    select
        s.*
      , row_number() over (partition by s.person_id, s.provider_id
order by s.step) as step_choice_rank
    from current_all_steps as s
  ) as d
  where step_choice_rank = 1
)

, yearly_all_steps as (
  -- Yearly windows are driven by performance_year (Jan..Dec for 12-month and Jan(Y-1)..Dec(Y) for expanded)
  select
      py.person_id
    , py.performance_year
    , c.provider_id
    , coalesce(c.provider_bucket, 'unknown') as provider_bucket
    , c.prov_specialty
    , 1 as step
    , sum(c.allowed_amount) as allowed_amount
    , count(distinct c.encounter_id) as visits
  from {{ ref('provider_attribution__int_person_years') }} as py
  inner join {{ ref('provider_attribution__int_primary_care_claims') }} as c
    on py.person_id = c.person_id
   and c.claim_year = py.performance_year
   and c.provider_id is not null
   and c.provider_bucket in ('pcp', 'npp')
  group by py.person_id, py.performance_year, c.provider_id, coalesce(c.provider_bucket, 'unknown'), c.prov_specialty

  union all
  select
      py.person_id
    , py.performance_year
    , c.provider_id
    , coalesce(c.provider_bucket, 'unknown') as provider_bucket
    , c.prov_specialty
    , 2 as step
    , sum(c.allowed_amount) as allowed_amount
    , count(distinct c.encounter_id) as visits
  from {{ ref('provider_attribution__int_person_years') }} as py
  inner join {{ ref('provider_attribution__int_primary_care_claims') }} as c
    on py.person_id = c.person_id
   and c.claim_year = py.performance_year
   and c.provider_id is not null
   and c.provider_bucket = 'specialist'
  group by py.person_id, py.performance_year, c.provider_id, coalesce(c.provider_bucket, 'unknown'), c.prov_specialty

  union all
  select
      py.person_id
    , py.performance_year
    , c.provider_id
    , coalesce(c.provider_bucket, 'unknown') as provider_bucket
    , c.prov_specialty
    , 3 as step
    , sum(c.allowed_amount) as allowed_amount
    , count(distinct c.encounter_id) as visits
  from {{ ref('provider_attribution__int_person_years') }} as py
  inner join {{ ref('provider_attribution__int_primary_care_claims') }} as c
    on py.person_id = c.person_id
   and c.claim_year_month_int between ((py.performance_year - 1) * 100 + 1)
                                  and (py.performance_year * 100 + 12)
   and c.provider_id is not null
   and c.provider_bucket in ('pcp', 'npp')
  group by py.person_id, py.performance_year, c.provider_id, coalesce(c.provider_bucket, 'unknown'), c.prov_specialty

  union all
  select
      py.person_id
    , py.performance_year
    , c.provider_id
    , coalesce(c.provider_bucket, 'unknown') as provider_bucket
    , c.prov_specialty
    , 4 as step
    , sum(c.allowed_amount) as allowed_amount
    , count(distinct c.encounter_id) as visits
  from {{ ref('provider_attribution__int_person_years') }} as py
  inner join {{ ref('provider_attribution__int_primary_care_claims') }} as c
    on py.person_id = c.person_id
   and c.claim_year_month_int between ((py.performance_year - 1) * 100 + 1)
                                  and (py.performance_year * 100 + 12)
   and c.provider_id is not null
  group by py.person_id, py.performance_year, c.provider_id, coalesce(c.provider_bucket, 'unknown'), c.prov_specialty

  union all
  select
      py.person_id
    , py.performance_year
    , arc.provider_id
    , coalesce(arc.provider_bucket, 'unknown') as provider_bucket
    , arc.prov_specialty
    , 5 as step
    , sum(arc.allowed_amount) as allowed_amount
    , count(distinct arc.encounter_id) as visits
  from {{ ref('provider_attribution__int_person_years') }} as py
  inner join (
    select
        e.person_id
      , e.provider_id
      , e.encounter_id
      , e.claim_year_month_int
      , e.allowed_amount
      , coalesce(pc.provider_bucket, 'other_individual') as provider_bucket
      , coalesce(pc.prov_specialty, sp.primary_specialty_description) as prov_specialty
    from eligible_all_claims as e
    inner join {{ ref('provider_attribution__stg_terminology__provider') }} as sp
      on cast(e.provider_id as {{ dbt.type_string() }}) = cast(sp.npi as {{ dbt.type_string() }})
     and lower(trim(sp.entity_type_description)) = 'individual'
    left outer join {{ ref('provider_attribution__provider_classification') }} as pc
      on e.provider_id = pc.provider_id
  ) as arc
    on py.person_id = arc.person_id
   and arc.claim_year_month_int between ((py.performance_year - 1) * 100 + 1)
                                   and (py.performance_year * 100 + 12)
  where arc.provider_id is not null
  group by py.person_id, py.performance_year, arc.provider_id, coalesce(arc.provider_bucket, 'unknown'), arc.prov_specialty
)

, yearly_unique as (
  select * from (
    select
        s.*
      , row_number() over (
          partition by s.person_id, s.performance_year, s.provider_id
          order by s.step
        ) as step_choice_rank
    from yearly_all_steps as s
  ) as d
  where step_choice_rank = 1
)

, yearly as (
  select
      y.person_id
    , cast(y.performance_year as {{ dbt.type_int() }}) as performance_year
    , cast(null as date) as as_of_date
    , y.provider_id
    , y.provider_bucket
    , y.prov_specialty
    , y.step
    , case y.step
        when 1 then '12-month PCP/NPP primary-care HCPCS'
        when 2 then '12-month specialist primary-care HCPCS'
        when 3 then '24-month PCP/NPP primary-care HCPCS'
        when 4 then '24-month primary-care HCPCS (any classification)'
        when 5 then '24-month any rendering NPI'
        else 'Unknown'
      end as step_description
    , y.allowed_amount
    , y.visits
    , 'yearly' as scope
    , case
        when y.step in (3, 4, 5)
          then coalesce(start_prev.first_day_of_month, start_curr.first_day_of_month)
        else start_curr.first_day_of_month
      end as lookback_start_date
    , end_curr.last_day_of_month as lookback_end_date
    , {{ concat_custom(["'yearly|'", "cast(y.performance_year as " ~ dbt.type_string() ~ ")", "'|'", "y.person_id"]) }} as attribution_key
    , rank() over (partition by y.person_id, y.performance_year
                   order by y.step asc, y.allowed_amount desc, y.visits desc, y.provider_id) as ranking
  from yearly_unique as y
  left outer join calendar_months as start_curr
    on start_curr.year_month_int = (y.performance_year * 100) + 1
  left outer join calendar_months as start_prev
    on start_prev.year_month_int = ((y.performance_year - 1) * 100) + 1
  left outer join calendar_months as end_curr
    on end_curr.year_month_int = (y.performance_year * 100) + 12
)

, current_scope as (
  select
      s.person_id
    , null as performance_year
    , p.as_of_date
    , s.provider_id
    , s.provider_bucket
    , s.prov_specialty
    , s.step
    , case s.step
        when 1 then '12-month PCP/NPP primary-care HCPCS'
        when 2 then '12-month specialist primary-care HCPCS'
        when 3 then '24-month PCP/NPP primary-care HCPCS'
        when 4 then '24-month primary-care HCPCS (any classification)'
        when 5 then '24-month any rendering NPI'
        else 'Unknown'
      end as step_description
    , s.allowed_amount
    , s.visits
    , 'current' as scope
    , case when s.step in (1, 2) then lb.lookback_start_date_12 else lb.lookback_start_date_24 end as lookback_start_date
    , p.as_of_date as lookback_end_date
    , {{ concat_custom(["'current|'", "replace(cast(p.as_of_date as " ~ dbt.type_string() ~ "),'-','')", "'|'", "s.person_id"]) }} as attribution_key
    , rank() over (partition by s.person_id
                   order by s.step asc, s.allowed_amount desc, s.visits desc, s.provider_id) as ranking
  from current_unique as s
  inner join eligible_current as ec
    on s.person_id = ec.person_id
  cross join params as p
  cross join lookback_bounds as lb
)

select
    person_id
  , performance_year
  , as_of_date
  , provider_id
  , provider_bucket
  , prov_specialty
  , step
  , step_description
  , allowed_amount
  , visits
  , scope
  , lookback_start_date
  , lookback_end_date
  , ranking
  , attribution_key
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from yearly

union all

select
    person_id
  , performance_year
  , as_of_date
  , provider_id
  , provider_bucket
  , prov_specialty
  , step
  , step_description
  , allowed_amount
  , visits
  , scope
  , lookback_start_date
  , lookback_end_date
  , ranking
  , attribution_key
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from current_scope
