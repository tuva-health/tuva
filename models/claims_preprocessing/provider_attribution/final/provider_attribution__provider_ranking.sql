{{ config(
     enabled = (var('provider_attribution_enabled', False) and var('claims_enabled', False))
   )
}}

-- Build a comprehensive provider ranking that shows all potential providers
-- a beneficiary could be attributed to, along with the earliest step each
-- provider qualifies for. Final assignment models can then pick rank = 1.

with member_months as (
  select
      person_id
    , year_month
    , data_source
  from {{ ref('member_month__member_month') }}
  group by
      person_id
    , year_month
    , data_source
)

, calendar_months as (
  select distinct
      year_month_int
    , first_day_of_month
    , last_day_of_month
  from {{ ref('member_month__month_spine') }}
)

, claim_bounds as (
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

-- Rolling “current” window helpers
, months_12 as (
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

, lookback_12 as (
  select
      data_source
    , min(first_day_of_month) as lookback_start_date_12
  from months_12
  group by data_source
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
      l12.data_source
    , l12.lookback_start_date_12
    , l24.lookback_start_date_24
  from lookback_12 as l12
  inner join lookback_24 as l24
    on l12.data_source = l24.data_source
)

, eligible_current as (
  -- Current-scope eligibility: beneficiaries with at least one member month
  -- within the 12-month window ending at as_of_date.
  select distinct
      mm.person_id
    , mm.data_source
  from member_months as mm
  inner join months_12 as m
    on mm.data_source = m.data_source
   and mm.year_month = cast(m.year_month_int as {{ dbt.type_string() }})
)

, claims_12 as (
  select
      c.person_id
    , c.data_source
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
    on c.data_source = m.data_source
   and c.claim_year_month_int = m.year_month_int
  inner join params as p
    on c.data_source = p.data_source
  where c.claim_end_date <= p.as_of_date
)

, claims_24 as (
  select
      c.person_id
    , c.data_source
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
    on c.data_source = m.data_source
   and c.claim_year_month_int = m.year_month_int
  inner join params as p
    on c.data_source = p.data_source
  where c.claim_end_date <= p.as_of_date
)

, all_claim_month as (
  select
      mc.person_id
    , mc.data_source
    , mc.claim_id
    , mc.claim_line_number
    , cast(mc.encounter_id as {{ dbt.type_string() }}) as encounter_id
    , mc.claim_start_date
    , mc.claim_end_date
    , cast({{ yyyymm('mc.claim_start_date') }} as {{ dbt.type_int() }}) as claim_year_month_int
    , {{ yyyymm('mc.claim_start_date') }} as claim_year_month
    , coalesce(nullif(mc.allowed_amount, 0), mc.paid_amount, 0) as allowed_amount
    , cast(mc.rendering_npi as {{ dbt.type_string() }}) as provider_id
  from {{ ref('provider_attribution__stg_medical_claim') }} as mc
)

, eligible_all_claims as (
  select ac.*
  from all_claim_month as ac
  inner join member_months as mm
    on ac.person_id = mm.person_id
   and ac.data_source = mm.data_source
   and ac.claim_year_month = mm.year_month
)

, all_rendering_claims as (
  select
      e.person_id
    , e.data_source
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
  inner join {{ ref('provider_data__provider') }} as sp
    on cast(e.provider_id as {{ dbt.type_string() }}) = cast(sp.npi as {{ dbt.type_string() }})
   and lower(trim(sp.entity_type_description)) = 'individual'
  left outer join {{ ref('provider_attribution__provider_classification') }} as pc
    on e.provider_id = pc.provider_id
)

-- Build all potential providers (no bene-level gating across steps), then
-- collapse to the earliest qualifying step per person/provider.
, current_all_steps as (
  select person_id, data_source, provider_id, provider_bucket, prov_specialty, 1 as step
         , sum(allowed_amount) as allowed_amount
         , count(distinct encounter_id) as visits
  from claims_12
  where provider_id is not null and provider_bucket in ('pcp', 'npp')
  group by person_id, data_source, provider_id, provider_bucket, prov_specialty

  union all
  select person_id, data_source, provider_id, provider_bucket, prov_specialty, 2 as step
         , sum(allowed_amount) as allowed_amount
         , count(distinct encounter_id) as visits
  from claims_12
  where provider_id is not null and provider_bucket = 'specialist'
  group by person_id, data_source, provider_id, provider_bucket, prov_specialty

  union all
  select person_id, data_source, provider_id, provider_bucket, prov_specialty, 3 as step
         , sum(allowed_amount) as allowed_amount
         , count(distinct encounter_id) as visits
  from claims_24
  where provider_id is not null and provider_bucket in ('pcp', 'npp')
  group by person_id, data_source, provider_id, provider_bucket, prov_specialty

  union all
  select person_id, data_source, provider_id, provider_bucket, prov_specialty, 4 as step
         , sum(allowed_amount) as allowed_amount
         , count(distinct encounter_id) as visits
  from claims_24
  where provider_id is not null
  group by person_id, data_source, provider_id, provider_bucket, prov_specialty

  union all
  select arc.person_id, arc.data_source, arc.provider_id, coalesce(arc.provider_bucket, 'unknown') as provider_bucket
         , arc.prov_specialty, 5 as step
         , sum(arc.allowed_amount) as allowed_amount
         , count(distinct arc.encounter_id) as visits
  from all_rendering_claims as arc
  inner join months_24 as m
    on arc.data_source = m.data_source
   and arc.claim_year_month_int = m.year_month_int
  inner join params as p
    on arc.data_source = p.data_source
  where arc.provider_id is not null and arc.claim_end_date <= p.as_of_date
  group by arc.person_id, arc.data_source, arc.provider_id, coalesce(arc.provider_bucket, 'unknown'), arc.prov_specialty
)

, current_unique as (
  select *
  from (
    select
        s.*
      , row_number() over (partition by s.person_id, s.provider_id
                                        , s.data_source
order by s.step) as step_choice_rank
    from current_all_steps as s
  ) as d
  where step_choice_rank = 1
)

, yearly_all_steps as (
  -- Yearly windows are driven by performance_year (Jan..Dec for 12-month and Jan(Y-1)..Dec(Y) for expanded)
  select
      py.person_id
    , py.data_source
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
   and py.data_source = c.data_source
   and c.claim_year = py.performance_year
   and c.provider_id is not null
   and c.provider_bucket in ('pcp', 'npp')
  group by py.person_id, py.data_source, py.performance_year, c.provider_id, coalesce(c.provider_bucket, 'unknown'), c.prov_specialty

  union all
  select
      py.person_id
    , py.data_source
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
   and py.data_source = c.data_source
   and c.claim_year = py.performance_year
   and c.provider_id is not null
   and c.provider_bucket = 'specialist'
  group by py.person_id, py.data_source, py.performance_year, c.provider_id, coalesce(c.provider_bucket, 'unknown'), c.prov_specialty

  union all
  select
      py.person_id
    , py.data_source
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
   and py.data_source = c.data_source
   and c.claim_year_month_int between ((py.performance_year - 1) * 100 + 1)
                                  and (py.performance_year * 100 + 12)
   and c.provider_id is not null
   and c.provider_bucket in ('pcp', 'npp')
  group by py.person_id, py.data_source, py.performance_year, c.provider_id, coalesce(c.provider_bucket, 'unknown'), c.prov_specialty

  union all
  select
      py.person_id
    , py.data_source
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
   and py.data_source = c.data_source
   and c.claim_year_month_int between ((py.performance_year - 1) * 100 + 1)
                                  and (py.performance_year * 100 + 12)
   and c.provider_id is not null
  group by py.person_id, py.data_source, py.performance_year, c.provider_id, coalesce(c.provider_bucket, 'unknown'), c.prov_specialty

  union all
  select
      py.person_id
    , py.data_source
    , py.performance_year
    , arc.provider_id
    , coalesce(arc.provider_bucket, 'unknown') as provider_bucket
    , arc.prov_specialty
    , 5 as step
    , sum(arc.allowed_amount) as allowed_amount
    , count(distinct arc.encounter_id) as visits
  from {{ ref('provider_attribution__int_person_years') }} as py
  inner join all_rendering_claims as arc
    on py.person_id = arc.person_id
   and py.data_source = arc.data_source
   and arc.claim_year_month_int between ((py.performance_year - 1) * 100 + 1)
                                   and (py.performance_year * 100 + 12)
  where arc.provider_id is not null
  group by py.person_id, py.data_source, py.performance_year, arc.provider_id, coalesce(arc.provider_bucket, 'unknown'), arc.prov_specialty
)

, yearly_unique as (
  select * from (
    select
        s.*
      , row_number() over (
          partition by s.person_id, s.performance_year, s.provider_id
          , s.data_source
          order by s.step
        ) as step_choice_rank
    from yearly_all_steps as s
  ) as d
  where step_choice_rank = 1
)

, yearly as (
  select
      y.person_id
    , y.data_source
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
    , {{ concat_custom(["'yearly|'", "y.data_source", "'|'", "cast(y.performance_year as " ~ dbt.type_string() ~ ")", "'|'", "y.person_id"]) }} as attribution_key
    , rank() over (partition by y.person_id, y.data_source, y.performance_year
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
    , s.data_source
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
    , {{ concat_custom(["'current|'", "s.data_source", "'|'", "replace(cast(p.as_of_date as " ~ dbt.type_string() ~ "),'-','')", "'|'", "s.person_id"]) }} as attribution_key
    , rank() over (partition by s.person_id, s.data_source
                   order by s.step asc, s.allowed_amount desc, s.visits desc, s.provider_id) as ranking
  from current_unique as s
  inner join eligible_current as ec
    on s.person_id = ec.person_id
   and s.data_source = ec.data_source
  inner join params as p
    on s.data_source = p.data_source
  inner join lookback_bounds as lb
    on s.data_source = lb.data_source
)

select
    person_id
  , data_source
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
  , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from yearly

union all

select
    person_id
  , data_source
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
  , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from current_scope
