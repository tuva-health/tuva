{{
    config(
        enabled = var('benchmarks_already_created', false) | as_bool
    )
}}

with expected_member_month as (
    select 
          p.benchmark_key
        , py.year_nbr
        , py.person_id
        , py.payer
        , py.{{ quote_column('plan') }}
        , py.data_source
        , p.paid_amount_pred/py.member_month_count as paid_amount_pred
        , p.outpatient_paid_amount_pred/py.member_month_count as outpatient_paid_amount_pred
        , p.other_paid_amount_pred/py.member_month_count as other_paid_amount_pred
        , p.office_based_paid_amount_pred/py.member_month_count as office_based_paid_amount_pred
        , p.inpatient_paid_amount_pred/py.member_month_count as inpatient_paid_amount_pred

        , p.inpatient_count_pred/py.member_month_count as inpatient_encounter_count_pred
        , p.office_based_count_pred/py.member_month_count as office_based_encounter_count_pred
        , p.other_count_pred/py.member_month_count as other_encounter_count_pred
        , p.outpatient_count_pred/py.member_month_count as outpatient_encounter_count_pred

        , p.acute_inpatient_count_pred/py.member_month_count as acute_inpatient_encounter_count_pred
        , p.emergency_department_count_pred/py.member_month_count as ed_encounter_count_pred
        , p.inpatient_skilled_nursing_count_pred/py.member_month_count as inpatient_skilled_nursing_encounter_count_pred
        , p.office_visit_count_pred/py.member_month_count as office_visit_encounter_count_pred
        , p.home_health_count_pred/py.member_month_count as home_health_encounter_count_pred
        , p.inpatient_skilled_nursing_count_pred/py.member_month_count as snf_encounter_count_pred

    from {{ var('predictions_person_year') }} p 
    inner join {{ ref('benchmarks__person_year') }} py on p.benchmark_key = py.benchmark_key
)

, claim as (
    select
        mc.person_id
      , mc.data_source
      , mc.payer
      , mc.{{ quote_column('plan') }}
      , cal.year_month_int as year_month
      , sum(mc.paid_amount) as paid_amount
      , sum(case when mc.encounter_group = 'inpatient' then mc.paid_amount else 0 end) as inpatient_paid_amount_actual
      , sum(case when mc.encounter_group = 'outpatient' then mc.paid_amount else 0 end) as outpatient_paid_amount_actual
      , sum(case when mc.encounter_group = 'other' then mc.paid_amount else 0 end) as other_paid_amount_actual
      , sum(case when mc.encounter_group = 'office based' then mc.paid_amount else 0 end) as office_based_paid_amount_actual
      
    from {{ ref('core__medical_claim') }} as mc
    inner join {{ ref('reference_data__calendar') }} as cal 
      on mc.claim_end_date = cal.full_date
    inner join {{ ref('core__member_months') }} as mm 
      on mc.person_id = mm.person_id
      and mc.data_source = mm.data_source
      and mc.payer = mm.payer
      and mc.{{ quote_column('plan') }} = mm.{{ quote_column('plan') }}
      and cal.year_month_int = mm.year_month
    group by
        mc.person_id
      , mc.data_source
      , mc.payer
      , mc.{{ quote_column('plan') }}
      , cal.year_month_int 
)

, encounters as (
    select
        e.person_id
      , e.data_source
      , mc.payer
      , mc.{{ quote_column('plan') }}
      , cal.year_month_int as year_month
      , count(distinct case when e.encounter_group = 'inpatient' then e.encounter_id else null end) as inpatient_encounter_count
      , count(distinct case when e.encounter_group = 'outpatient' then e.encounter_id else null end) as outpatient_encounter_count
      , count(distinct case when e.encounter_group = 'office based' then e.encounter_id else null end) as office_based_encounter_count
      , count(distinct case when e.encounter_group = 'other' then e.encounter_id else null end) as other_encounter_count
      , count(distinct case when e.encounter_type = 'acute inpatient' then e.encounter_id else null end) as acute_inpatient_encounter_count
      , count(distinct case when e.encounter_type = 'inpatient skilled nursing' then e.encounter_id else null end) as inpatient_skilled_nursing_encounter_count
      , count(distinct case when e.encounter_type = 'emergency department' then e.encounter_id else null end) as ed_encounter_count
      , count(distinct case when e.encounter_type = 'office visit' then e.encounter_id else null end) as office_visit_encounter_count
      , count(distinct case when e.encounter_type = 'home health' then e.encounter_id else null end) as home_health_encounter_count
      , count(distinct case when e.encounter_type = 'inpatient skilled nursing' then e.encounter_id else null end) as snf_encounter_count

    from {{ ref('core__medical_claim') }} as mc
    inner join {{ ref('core__encounter') }} as e 
      on e.encounter_id = mc.encounter_id
    inner join {{ ref('reference_data__calendar') }} as cal 
      on e.encounter_start_date = cal.full_date
    inner join {{ ref('core__member_months') }} as mm 
      on mc.person_id = mm.person_id
      and mc.data_source = mm.data_source
      and mc.payer = mm.payer
      and mc.{{ quote_column('plan') }} = mm.{{ quote_column('plan') }}
      and cal.year_month_int = mm.year_month
    group by
        e.person_id
      , e.data_source
      , mc.payer
      , mc.{{ quote_column('plan') }}
      , cal.year_month_int 
)

, member_month as (
  select 
    person_id
  , payer
  , {{ quote_column('plan') }}
  , data_source
  , year_month
  , left(year_month,4) as year_nbr
  , 1 as member_month_count
  from {{ ref('core__member_months') }} as mm
  group by 
    person_id
  , payer
  , {{ quote_column('plan') }}
  , data_source
  , year_month
)

, cal as (
  select distinct 
    year_month_int as year_month
  , first_day_of_month
  from {{ ref('reference_data__calendar') }} 
)

select
   mm.year_month
 , cal.first_day_of_month
 , mm.person_id
 , mm.payer
 , mm.{{ quote_column('plan') }}
 , mm.data_source
 , emm.benchmark_key
 , '{{ var('tuva_last_run') }}' as tuva_last_run
   -- ==== ACTUAL PMPM paid amounts ====
 , coalesce(c.paid_amount,                  0) as actual_paid_amount
 , coalesce(c.inpatient_paid_amount_actual, 0) as actual_inpatient_paid_amount
 , coalesce(c.outpatient_paid_amount_actual,0) as actual_outpatient_paid_amount
 , coalesce(c.office_based_paid_amount_actual,0) as actual_office_based_paid_amount
 , coalesce(c.other_paid_amount_actual,     0) as actual_other_paid_amount

   -- ==== EXPECTED (PRED) PMPM paid amounts ====
 , coalesce(emm.paid_amount_pred,           0) as expected_paid_amount
 , coalesce(emm.inpatient_paid_amount_pred, 0) as expected_inpatient_paid_amount
 , coalesce(emm.outpatient_paid_amount_pred,0) as expected_outpatient_paid_amount
 , coalesce(emm.office_based_paid_amount_pred,0) as expected_office_based_paid_amount
 , coalesce(emm.other_paid_amount_pred,     0) as expected_other_paid_amount

   -- ==== ACTUAL Encounter counts (monthly per person) ====
 , coalesce(enc.inpatient_encounter_count, 0) as actual_inpatient_encounter_count
 , coalesce(enc.outpatient_encounter_count, 0) as actual_outpatient_encounter_count
 , coalesce(enc.office_based_encounter_count, 0) as actual_office_based_encounter_count
 , coalesce(enc.other_encounter_count, 0) as actual_other_encounter_count
 , coalesce(enc.acute_inpatient_encounter_count, 0) as actual_acute_inpatient_encounter_count
 , coalesce(enc.ed_encounter_count, 0) as actual_ed_encounter_count
 , coalesce(enc.inpatient_skilled_nursing_encounter_count, 0) as actual_inpatient_skilled_nursing_encounter_count
 , coalesce(enc.office_visit_encounter_count, 0) as actual_office_visit_encounter_count
 , coalesce(enc.home_health_encounter_count, 0) as actual_home_health_encounter_count
 , coalesce(enc.snf_encounter_count, 0) as actual_snf_encounter_count

   -- ==== EXPECTED (PRED) Encounter counts =============
 , coalesce(emm.inpatient_encounter_count_pred, 0) as expected_inpatient_encounter_count
 , coalesce(emm.outpatient_encounter_count_pred, 0) as expected_outpatient_encounter_count
 , coalesce(emm.office_based_encounter_count_pred, 0) as expected_office_based_encounter_count
 , coalesce(emm.other_encounter_count_pred, 0) as expected_other_encounter_count
 , coalesce(emm.acute_inpatient_encounter_count_pred, 0) as expected_acute_inpatient_encounter_count
 , coalesce(emm.ed_encounter_count_pred, 0) as expected_ed_encounter_count
 , coalesce(emm.inpatient_skilled_nursing_encounter_count_pred, 0) as expected_inpatient_skilled_nursing_encounter_count
 , coalesce(emm.office_visit_encounter_count_pred, 0) as expected_office_visit_encounter_count
 , coalesce(emm.home_health_encounter_count_pred, 0) as expected_home_health_encounter_count
 , coalesce(emm.snf_encounter_count_pred, 0) as expected_snf_encounter_count

from member_month as mm
inner join cal
  on mm.year_month = cal.year_month

left join claim as c 
  on mm.person_id    = c.person_id
  and mm.data_source = c.data_source
  and mm.payer       = c.payer
  and mm.{{ quote_column('plan') }}        = c.{{ quote_column('plan') }}
  and mm.year_month  = c.year_month

inner join expected_member_month as emm
  on mm.year_nbr    = emm.year_nbr
  and mm.person_id  = emm.person_id
  and mm.payer      = emm.payer
  and mm.{{ quote_column('plan') }}       = emm.{{ quote_column('plan') }}
  and mm.data_source= emm.data_source

left join encounters as enc
  on mm.person_id    = enc.person_id
  and mm.data_source = enc.data_source
  and mm.payer       = enc.payer
  and mm.{{ quote_column('plan') }}        = enc.{{ quote_column('plan') }}
  and mm.year_month  = enc.year_month