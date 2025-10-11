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
        , p.paid_amount_pred as paid_amount_pred
        , p.outpatient_paid_amount_pred as outpatient_paid_amount_pred
        , p.other_paid_amount_pred as other_paid_amount_pred
        , p.office_based_paid_amount_pred as office_based_paid_amount_pred
        , p.inpatient_paid_amount_pred as inpatient_paid_amount_pred

        , p.inpatient_count_pred as inpatient_encounter_count_pred
        , p.office_based_count_pred as office_based_encounter_count_pred
        , p.other_count_pred as other_encounter_count_pred
        , p.outpatient_count_pred as outpatient_encounter_count_pred

        /*
        * All encounter type paid amount predictions
        */
        , p.acute_inpatient_paid_amount_pred
        , p.ambulance_orphaned_paid_amount_pred
        , p.ambulatory_surgery_center_paid_amount_pred
        , p.dialysis_paid_amount_pred
        , p.dme_orphaned_paid_amount_pred
        , p.emergency_department_paid_amount_pred
        , p.home_health_paid_amount_pred
        , p.inpatient_hospice_paid_amount_pred
        , 0 as inpatient_long_term_acute_care_paid_amount_pred  -- place holder
        , p.inpatient_psych_paid_amount_pred
        , p.inpatient_rehabilitation_paid_amount_pred
        , p.inpatient_skilled_nursing_paid_amount_pred
        , 0 as inpatient_substance_use_paid_amount_pred         -- place holder
        , p.lab_orphaned_paid_amount_pred
        , p.office_visit_paid_amount_pred
        , p.office_visit_other_paid_amount_pred
        , p.office_visit_injections_paid_amount_pred
        , p.office_visit_pt_ot_st_paid_amount_pred
        , p.office_visit_radiology_paid_amount_pred
        , p.office_visit_surgery_paid_amount_pred
        , p.orphaned_claim_paid_amount_pred
        , p.outpatient_hospice_paid_amount_pred
        , p.outpatient_hospital_or_clinic_paid_amount_pred
        , p.outpatient_injections_paid_amount_pred
        , p.outpatient_psych_paid_amount_pred
        , p.outpatient_pt_ot_st_paid_amount_pred
        , p.outpatient_radiology_paid_amount_pred
        , p.outpatient_rehabilitation_paid_amount_pred
        , 0 as outpatient_substance_use_paid_amount_pred        -- place holder
        , p.outpatient_surgery_paid_amount_pred
        , p.telehealth_paid_amount_pred
        , p.urgent_care_paid_amount_pred

        /*
        * All encounter type count predictions
        */
        , p.acute_inpatient_count_pred as acute_inpatient_encounter_count_pred
        , p.ambulance_orphaned_count_pred as ambulance_orphaned_encounter_count_pred
        , p.ambulatory_surgery_center_count_pred as ambulatory_surgery_center_encounter_count_pred
        , p.dialysis_count_pred as dialysis_encounter_count_pred
        , p.dme_orphaned_count_pred as dme_orphaned_encounter_count_pred
        , p.emergency_department_count_pred as emergency_department_encounter_count_pred
        , p.emergency_department_count_pred as ed_encounter_count_pred -- original alias
        , p.home_health_count_pred as home_health_encounter_count_pred
        , p.inpatient_hospice_count_pred as inpatient_hospice_encounter_count_pred
        , 0 as inpatient_long_term_acute_care_encounter_count_pred  -- place holder
        , p.inpatient_psych_count_pred as inpatient_psych_encounter_count_pred
        , p.inpatient_rehabilitation_count_pred as inpatient_rehabilitation_encounter_count_pred
        , p.inpatient_skilled_nursing_count_pred as inpatient_skilled_nursing_encounter_count_pred
        , p.inpatient_skilled_nursing_count_pred as snf_encounter_count_pred -- original alias
        , 0 as inpatient_substance_use_encounter_count_pred         -- place holder
        , p.lab_orphaned_count_pred as lab_orphaned_encounter_count_pred
        , p.office_visit_count_pred as office_visit_encounter_count_pred
        , p.office_visit_other_count_pred as office_visit_other_encounter_count_pred
        , p.office_visit_injections_count_pred as office_visit_injections_encounter_count_pred
        , p.office_visit_pt_ot_st_count_pred as office_visit_pt_ot_st_encounter_count_pred
        , p.office_visit_radiology_count_pred as office_visit_radiology_encounter_count_pred
        , p.office_visit_surgery_count_pred as office_visit_surgery_encounter_count_pred
        , p.orphaned_claim_count_pred as orphaned_claim_encounter_count_pred
        , p.outpatient_hospice_count_pred as outpatient_hospice_encounter_count_pred
        , p.outpatient_hospital_or_clinic_count_pred as outpatient_hospital_or_clinic_encounter_count_pred
        , p.outpatient_injections_count_pred as outpatient_injections_encounter_count_pred
        , p.outpatient_psych_count_pred as outpatient_psych_encounter_count_pred
        , p.outpatient_pt_ot_st_count_pred as outpatient_pt_ot_st_encounter_count_pred
        , p.outpatient_radiology_count_pred as outpatient_radiology_encounter_count_pred
        , p.outpatient_rehabilitation_count_pred as outpatient_rehabilitation_encounter_count_pred
        , 0 as outpatient_substance_use_encounter_count_pred        -- place holder
        , p.outpatient_surgery_count_pred as outpatient_surgery_encounter_count_pred
        , p.telehealth_count_pred as telehealth_encounter_count_pred
        , p.urgent_care_count_pred as urgent_care_encounter_count_pred

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

      /*
      * All encounter type paid amounts
      */
      , sum(case when mc.encounter_type = 'acute inpatient' then mc.paid_amount else 0 end) as acute_inpatient_paid_amount_actual
      , sum(case when mc.encounter_type = 'ambulance - orphaned' then mc.paid_amount else 0 end) as ambulance_orphaned_paid_amount_actual
      , sum(case when mc.encounter_type = 'ambulatory surgery center' then mc.paid_amount else 0 end) as ambulatory_surgery_center_paid_amount_actual
      , sum(case when mc.encounter_type = 'dialysis' then mc.paid_amount else 0 end) as dialysis_paid_amount_actual
      , sum(case when mc.encounter_type = 'dme - orphaned' then mc.paid_amount else 0 end) as dme_orphaned_paid_amount_actual
      , sum(case when mc.encounter_type = 'emergency department' then mc.paid_amount else 0 end) as emergency_department_paid_amount_actual
      , sum(case when mc.encounter_type = 'home health' then mc.paid_amount else 0 end) as home_health_paid_amount_actual
      , sum(case when mc.encounter_type = 'inpatient hospice' then mc.paid_amount else 0 end) as inpatient_hospice_paid_amount_actual
      , sum(case when mc.encounter_type = 'inpatient long term acute care' then mc.paid_amount else 0 end) as inpatient_long_term_acute_care_paid_amount_actual
      , sum(case when mc.encounter_type = 'inpatient psych' then mc.paid_amount else 0 end) as inpatient_psych_paid_amount_actual
      , sum(case when mc.encounter_type = 'inpatient rehabilitation' then mc.paid_amount else 0 end) as inpatient_rehabilitation_paid_amount_actual
      , sum(case when mc.encounter_type = 'inpatient skilled nursing' then mc.paid_amount else 0 end) as inpatient_skilled_nursing_paid_amount_actual
      , sum(case when mc.encounter_type = 'inpatient substance use' then mc.paid_amount else 0 end) as inpatient_substance_use_paid_amount_actual
      , sum(case when mc.encounter_type = 'lab - orphaned' then mc.paid_amount else 0 end) as lab_orphaned_paid_amount_actual
      , sum(case when mc.encounter_type = 'office visit' then mc.paid_amount else 0 end) as office_visit_paid_amount_actual
      , sum(case when mc.encounter_type = 'office visit - other' then mc.paid_amount else 0 end) as office_visit_other_paid_amount_actual
      , sum(case when mc.encounter_type = 'office visit injections' then mc.paid_amount else 0 end) as office_visit_injections_paid_amount_actual
      , sum(case when mc.encounter_type = 'office visit pt/ot/st' then mc.paid_amount else 0 end) as office_visit_pt_ot_st_paid_amount_actual
      , sum(case when mc.encounter_type = 'office visit radiology' then mc.paid_amount else 0 end) as office_visit_radiology_paid_amount_actual
      , sum(case when mc.encounter_type = 'office visit surgery' then mc.paid_amount else 0 end) as office_visit_surgery_paid_amount_actual
      , sum(case when mc.encounter_type = 'orphaned claim' then mc.paid_amount else 0 end) as orphaned_claim_paid_amount_actual
      , sum(case when mc.encounter_type = 'outpatient hospice' then mc.paid_amount else 0 end) as outpatient_hospice_paid_amount_actual
      , sum(case when mc.encounter_type = 'outpatient hospital or clinic' then mc.paid_amount else 0 end) as outpatient_hospital_or_clinic_paid_amount_actual
      , sum(case when mc.encounter_type = 'outpatient injections' then mc.paid_amount else 0 end) as outpatient_injections_paid_amount_actual
      , sum(case when mc.encounter_type = 'outpatient psych' then mc.paid_amount else 0 end) as outpatient_psych_paid_amount_actual
      , sum(case when mc.encounter_type = 'outpatient pt/ot/st' then mc.paid_amount else 0 end) as outpatient_pt_ot_st_paid_amount_actual
      , sum(case when mc.encounter_type = 'outpatient radiology' then mc.paid_amount else 0 end) as outpatient_radiology_paid_amount_actual
      , sum(case when mc.encounter_type = 'outpatient rehabilitation' then mc.paid_amount else 0 end) as outpatient_rehabilitation_paid_amount_actual
      , sum(case when mc.encounter_type = 'outpatient substance use' then mc.paid_amount else 0 end) as outpatient_substance_use_paid_amount_actual
      , sum(case when mc.encounter_type = 'outpatient surgery' then mc.paid_amount else 0 end) as outpatient_surgery_paid_amount_actual
      , sum(case when mc.encounter_type = 'telehealth' then mc.paid_amount else 0 end) as telehealth_paid_amount_actual
      , sum(case when mc.encounter_type = 'urgent care' then mc.paid_amount else 0 end) as urgent_care_paid_amount_actual

    from {{ ref('benchmarks__stg_core__medical_claim') }} as mc
    inner join {{ ref('benchmarks__stg_reference_data__calendar') }} as cal
      on mc.claim_end_date = cal.full_date
    inner join {{ ref('benchmarks__stg_core__member_months') }} as mm
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

      /*
      * All encounter type counts
      */
      , count(distinct case when e.encounter_type = 'acute inpatient' then e.encounter_id else null end) as acute_inpatient_encounter_count
      , count(distinct case when e.encounter_type = 'ambulance - orphaned' then e.encounter_id else null end) as ambulance_orphaned_encounter_count
      , count(distinct case when e.encounter_type = 'ambulatory surgery center' then e.encounter_id else null end) as ambulatory_surgery_center_encounter_count
      , count(distinct case when e.encounter_type = 'dialysis' then e.encounter_id else null end) as dialysis_encounter_count
      , count(distinct case when e.encounter_type = 'dme - orphaned' then e.encounter_id else null end) as dme_orphaned_encounter_count
      , count(distinct case when e.encounter_type = 'emergency department' then e.encounter_id else null end) as emergency_department_encounter_count
      , count(distinct case when e.encounter_type = 'emergency department' then e.encounter_id else null end) as ed_encounter_count -- original alias
      , count(distinct case when e.encounter_type = 'home health' then e.encounter_id else null end) as home_health_encounter_count
      , count(distinct case when e.encounter_type = 'inpatient hospice' then e.encounter_id else null end) as inpatient_hospice_encounter_count
      , count(distinct case when e.encounter_type = 'inpatient long term acute care' then e.encounter_id else null end) as inpatient_long_term_acute_care_encounter_count
      , count(distinct case when e.encounter_type = 'inpatient psych' then e.encounter_id else null end) as inpatient_psych_encounter_count
      , count(distinct case when e.encounter_type = 'inpatient rehabilitation' then e.encounter_id else null end) as inpatient_rehabilitation_encounter_count
      , count(distinct case when e.encounter_type = 'inpatient skilled nursing' then e.encounter_id else null end) as inpatient_skilled_nursing_encounter_count
      , count(distinct case when e.encounter_type = 'inpatient skilled nursing' then e.encounter_id else null end) as snf_encounter_count -- original alias
      , count(distinct case when e.encounter_type = 'inpatient substance use' then e.encounter_id else null end) as inpatient_substance_use_encounter_count
      , count(distinct case when e.encounter_type = 'lab - orphaned' then e.encounter_id else null end) as lab_orphaned_encounter_count
      , count(distinct case when e.encounter_type = 'office visit' then e.encounter_id else null end) as office_visit_encounter_count
      , count(distinct case when e.encounter_type = 'office visit - other' then e.encounter_id else null end) as office_visit_other_encounter_count
      , count(distinct case when e.encounter_type = 'office visit injections' then e.encounter_id else null end) as office_visit_injections_encounter_count
      , count(distinct case when e.encounter_type = 'office visit pt/ot/st' then e.encounter_id else null end) as office_visit_pt_ot_st_encounter_count
      , count(distinct case when e.encounter_type = 'office visit radiology' then e.encounter_id else null end) as office_visit_radiology_encounter_count
      , count(distinct case when e.encounter_type = 'office visit surgery' then e.encounter_id else null end) as office_visit_surgery_encounter_count
      , count(distinct case when e.encounter_type = 'orphaned claim' then e.encounter_id else null end) as orphaned_claim_encounter_count
      , count(distinct case when e.encounter_type = 'outpatient hospice' then e.encounter_id else null end) as outpatient_hospice_encounter_count
      , count(distinct case when e.encounter_type = 'outpatient hospital or clinic' then e.encounter_id else null end) as outpatient_hospital_or_clinic_encounter_count
      , count(distinct case when e.encounter_type = 'outpatient injections' then e.encounter_id else null end) as outpatient_injections_encounter_count
      , count(distinct case when e.encounter_type = 'outpatient psych' then e.encounter_id else null end) as outpatient_psych_encounter_count
      , count(distinct case when e.encounter_type = 'outpatient pt/ot/st' then e.encounter_id else null end) as outpatient_pt_ot_st_encounter_count
      , count(distinct case when e.encounter_type = 'outpatient radiology' then e.encounter_id else null end) as outpatient_radiology_encounter_count
      , count(distinct case when e.encounter_type = 'outpatient rehabilitation' then e.encounter_id else null end) as outpatient_rehabilitation_encounter_count
      , count(distinct case when e.encounter_type = 'outpatient substance use' then e.encounter_id else null end) as outpatient_substance_use_encounter_count
      , count(distinct case when e.encounter_type = 'outpatient surgery' then e.encounter_id else null end) as outpatient_surgery_encounter_count
      , count(distinct case when e.encounter_type = 'telehealth' then e.encounter_id else null end) as telehealth_encounter_count
      , count(distinct case when e.encounter_type = 'urgent care' then e.encounter_id else null end) as urgent_care_encounter_count

    from {{ ref('benchmarks__stg_core__medical_claim') }} as mc
    inner join {{ ref('benchmarks__stg_core__encounter') }} as e
      on e.encounter_id = mc.encounter_id
    inner join {{ ref('benchmarks__stg_reference_data__calendar') }} as cal
      on e.encounter_start_date = cal.full_date
    inner join {{ ref('benchmarks__stg_core__member_months') }} as mm
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
  from {{ ref('benchmarks__stg_core__member_months') }} as mm
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
  from {{ ref('benchmarks__stg_reference_data__calendar') }}
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
   -- ==== ACTUAL PMPM paid amounts (Encounter Groups) ====
 , coalesce(c.paid_amount,                  0) as actual_paid_amount
 , coalesce(c.inpatient_paid_amount_actual, 0) as actual_inpatient_paid_amount
 , coalesce(c.outpatient_paid_amount_actual,0) as actual_outpatient_paid_amount
 , coalesce(c.office_based_paid_amount_actual,0) as actual_office_based_paid_amount
 , coalesce(c.other_paid_amount_actual,     0) as actual_other_paid_amount

   -- ==== EXPECTED (PRED) PMPM paid amounts (Encounter Groups) ====
 , coalesce(emm.paid_amount_pred,           0) as expected_paid_amount
 , coalesce(emm.inpatient_paid_amount_pred, 0) as expected_inpatient_paid_amount
 , coalesce(emm.outpatient_paid_amount_pred,0) as expected_outpatient_paid_amount
 , coalesce(emm.office_based_paid_amount_pred,0) as expected_office_based_paid_amount
 , coalesce(emm.other_paid_amount_pred,     0) as expected_other_paid_amount

   -- ==== ACTUAL Encounter counts (Encounter Groups) ====
 , coalesce(enc.inpatient_encounter_count, 0) as actual_inpatient_encounter_count
 , coalesce(enc.outpatient_encounter_count, 0) as actual_outpatient_encounter_count
 , coalesce(enc.office_based_encounter_count, 0) as actual_office_based_encounter_count
 , coalesce(enc.other_encounter_count, 0) as actual_other_encounter_count

   -- ==== EXPECTED (PRED) Encounter counts (Encounter Groups) =============
 , coalesce(emm.inpatient_encounter_count_pred, 0) as expected_inpatient_encounter_count
 , coalesce(emm.outpatient_encounter_count_pred, 0) as expected_outpatient_encounter_count
 , coalesce(emm.office_based_encounter_count_pred, 0) as expected_office_based_encounter_count
 , coalesce(emm.other_encounter_count_pred, 0) as expected_other_encounter_count

   -- ==== ACTUAL PMPM paid amounts (Encounter Types) ====
 , coalesce(c.acute_inpatient_paid_amount_actual, 0) as actual_acute_inpatient_paid_amount
 , coalesce(c.ambulance_orphaned_paid_amount_actual, 0) as actual_ambulance_orphaned_paid_amount
 , coalesce(c.ambulatory_surgery_center_paid_amount_actual, 0) as actual_ambulatory_surgery_center_paid_amount
 , coalesce(c.dialysis_paid_amount_actual, 0) as actual_dialysis_paid_amount
 , coalesce(c.dme_orphaned_paid_amount_actual, 0) as actual_dme_orphaned_paid_amount
 , coalesce(c.emergency_department_paid_amount_actual, 0) as actual_emergency_department_paid_amount
 , coalesce(c.home_health_paid_amount_actual, 0) as actual_home_health_paid_amount
 , coalesce(c.inpatient_hospice_paid_amount_actual, 0) as actual_inpatient_hospice_paid_amount
 , coalesce(c.inpatient_long_term_acute_care_paid_amount_actual, 0) as actual_inpatient_long_term_acute_care_paid_amount
 , coalesce(c.inpatient_psych_paid_amount_actual, 0) as actual_inpatient_psych_paid_amount
 , coalesce(c.inpatient_rehabilitation_paid_amount_actual, 0) as actual_inpatient_rehabilitation_paid_amount
 , coalesce(c.inpatient_skilled_nursing_paid_amount_actual, 0) as actual_inpatient_skilled_nursing_paid_amount
 , coalesce(c.inpatient_substance_use_paid_amount_actual, 0) as actual_inpatient_substance_use_paid_amount
 , coalesce(c.lab_orphaned_paid_amount_actual, 0) as actual_lab_orphaned_paid_amount
 , coalesce(c.office_visit_paid_amount_actual, 0) as actual_office_visit_paid_amount
 , coalesce(c.office_visit_other_paid_amount_actual, 0) as actual_office_visit_other_paid_amount
 , coalesce(c.office_visit_injections_paid_amount_actual, 0) as actual_office_visit_injections_paid_amount
 , coalesce(c.office_visit_pt_ot_st_paid_amount_actual, 0) as actual_office_visit_pt_ot_st_paid_amount
 , coalesce(c.office_visit_radiology_paid_amount_actual, 0) as actual_office_visit_radiology_paid_amount
 , coalesce(c.office_visit_surgery_paid_amount_actual, 0) as actual_office_visit_surgery_paid_amount
 , coalesce(c.orphaned_claim_paid_amount_actual, 0) as actual_orphaned_claim_paid_amount
 , coalesce(c.outpatient_hospice_paid_amount_actual, 0) as actual_outpatient_hospice_paid_amount
 , coalesce(c.outpatient_hospital_or_clinic_paid_amount_actual, 0) as actual_outpatient_hospital_or_clinic_paid_amount
 , coalesce(c.outpatient_injections_paid_amount_actual, 0) as actual_outpatient_injections_paid_amount
 , coalesce(c.outpatient_psych_paid_amount_actual, 0) as actual_outpatient_psych_paid_amount
 , coalesce(c.outpatient_pt_ot_st_paid_amount_actual, 0) as actual_outpatient_pt_ot_st_paid_amount
 , coalesce(c.outpatient_radiology_paid_amount_actual, 0) as actual_outpatient_radiology_paid_amount
 , coalesce(c.outpatient_rehabilitation_paid_amount_actual, 0) as actual_outpatient_rehabilitation_paid_amount
 , coalesce(c.outpatient_substance_use_paid_amount_actual, 0) as actual_outpatient_substance_use_paid_amount
 , coalesce(c.outpatient_surgery_paid_amount_actual, 0) as actual_outpatient_surgery_paid_amount
 , coalesce(c.telehealth_paid_amount_actual, 0) as actual_telehealth_paid_amount
 , coalesce(c.urgent_care_paid_amount_actual, 0) as actual_urgent_care_paid_amount

   -- ==== EXPECTED (PRED) PMPM paid amounts (Encounter Types) ====
 , coalesce(emm.acute_inpatient_paid_amount_pred, 0) as expected_acute_inpatient_paid_amount
 , coalesce(emm.ambulance_orphaned_paid_amount_pred, 0) as expected_ambulance_orphaned_paid_amount
 , coalesce(emm.ambulatory_surgery_center_paid_amount_pred, 0) as expected_ambulatory_surgery_center_paid_amount
 , coalesce(emm.dialysis_paid_amount_pred, 0) as expected_dialysis_paid_amount
 , coalesce(emm.dme_orphaned_paid_amount_pred, 0) as expected_dme_orphaned_paid_amount
 , coalesce(emm.emergency_department_paid_amount_pred, 0) as expected_emergency_department_paid_amount
 , coalesce(emm.home_health_paid_amount_pred, 0) as expected_home_health_paid_amount
 , coalesce(emm.inpatient_hospice_paid_amount_pred, 0) as expected_inpatient_hospice_paid_amount
 , coalesce(emm.inpatient_long_term_acute_care_paid_amount_pred, 0) as expected_inpatient_long_term_acute_care_paid_amount
 , coalesce(emm.inpatient_psych_paid_amount_pred, 0) as expected_inpatient_psych_paid_amount
 , coalesce(emm.inpatient_rehabilitation_paid_amount_pred, 0) as expected_inpatient_rehabilitation_paid_amount
 , coalesce(emm.inpatient_skilled_nursing_paid_amount_pred, 0) as expected_inpatient_skilled_nursing_paid_amount
 , coalesce(emm.inpatient_substance_use_paid_amount_pred, 0) as expected_inpatient_substance_use_paid_amount
 , coalesce(emm.lab_orphaned_paid_amount_pred, 0) as expected_lab_orphaned_paid_amount
 , coalesce(emm.office_visit_paid_amount_pred, 0) as expected_office_visit_paid_amount
 , coalesce(emm.office_visit_other_paid_amount_pred, 0) as expected_office_visit_other_paid_amount
 , coalesce(emm.office_visit_injections_paid_amount_pred, 0) as expected_office_visit_injections_paid_amount
 , coalesce(emm.office_visit_pt_ot_st_paid_amount_pred, 0) as expected_office_visit_pt_ot_st_paid_amount
 , coalesce(emm.office_visit_radiology_paid_amount_pred, 0) as expected_office_visit_radiology_paid_amount
 , coalesce(emm.office_visit_surgery_paid_amount_pred, 0) as expected_office_visit_surgery_paid_amount
 , coalesce(emm.orphaned_claim_paid_amount_pred, 0) as expected_orphaned_claim_paid_amount
 , coalesce(emm.outpatient_hospice_paid_amount_pred, 0) as expected_outpatient_hospice_paid_amount
 , coalesce(emm.outpatient_hospital_or_clinic_paid_amount_pred, 0) as expected_outpatient_hospital_or_clinic_paid_amount
 , coalesce(emm.outpatient_injections_paid_amount_pred, 0) as expected_outpatient_injections_paid_amount
 , coalesce(emm.outpatient_psych_paid_amount_pred, 0) as expected_outpatient_psych_paid_amount
 , coalesce(emm.outpatient_pt_ot_st_paid_amount_pred, 0) as expected_outpatient_pt_ot_st_paid_amount
 , coalesce(emm.outpatient_radiology_paid_amount_pred, 0) as expected_outpatient_radiology_paid_amount
 , coalesce(emm.outpatient_rehabilitation_paid_amount_pred, 0) as expected_outpatient_rehabilitation_paid_amount
 , coalesce(emm.outpatient_substance_use_paid_amount_pred, 0) as expected_outpatient_substance_use_paid_amount
 , coalesce(emm.outpatient_surgery_paid_amount_pred, 0) as expected_outpatient_surgery_paid_amount
 , coalesce(emm.telehealth_paid_amount_pred, 0) as expected_telehealth_paid_amount
 , coalesce(emm.urgent_care_paid_amount_pred, 0) as expected_urgent_care_paid_amount

   -- ==== ACTUAL Encounter counts (Encounter Types) ====
 , coalesce(enc.acute_inpatient_encounter_count, 0) as actual_acute_inpatient_encounter_count
 , coalesce(enc.ambulance_orphaned_encounter_count, 0) as actual_ambulance_orphaned_encounter_count
 , coalesce(enc.ambulatory_surgery_center_encounter_count, 0) as actual_ambulatory_surgery_center_encounter_count
 , coalesce(enc.dialysis_encounter_count, 0) as actual_dialysis_encounter_count
 , coalesce(enc.dme_orphaned_encounter_count, 0) as actual_dme_orphaned_encounter_count
 , coalesce(enc.emergency_department_encounter_count, 0) as actual_emergency_department_encounter_count
 , coalesce(enc.ed_encounter_count, 0) as actual_ed_encounter_count
 , coalesce(enc.home_health_encounter_count, 0) as actual_home_health_encounter_count
 , coalesce(enc.inpatient_hospice_encounter_count, 0) as actual_inpatient_hospice_encounter_count
 , coalesce(enc.inpatient_long_term_acute_care_encounter_count, 0) as actual_inpatient_long_term_acute_care_encounter_count
 , coalesce(enc.inpatient_psych_encounter_count, 0) as actual_inpatient_psych_encounter_count
 , coalesce(enc.inpatient_rehabilitation_encounter_count, 0) as actual_inpatient_rehabilitation_encounter_count
 , coalesce(enc.inpatient_skilled_nursing_encounter_count, 0) as actual_inpatient_skilled_nursing_encounter_count
 , coalesce(enc.snf_encounter_count, 0) as actual_snf_encounter_count
 , coalesce(enc.inpatient_substance_use_encounter_count, 0) as actual_inpatient_substance_use_encounter_count
 , coalesce(enc.lab_orphaned_encounter_count, 0) as actual_lab_orphaned_encounter_count
 , coalesce(enc.office_visit_encounter_count, 0) as actual_office_visit_encounter_count
 , coalesce(enc.office_visit_other_encounter_count, 0) as actual_office_visit_other_encounter_count
 , coalesce(enc.office_visit_injections_encounter_count, 0) as actual_office_visit_injections_encounter_count
 , coalesce(enc.office_visit_pt_ot_st_encounter_count, 0) as actual_office_visit_pt_ot_st_encounter_count
 , coalesce(enc.office_visit_radiology_encounter_count, 0) as actual_office_visit_radiology_encounter_count
 , coalesce(enc.office_visit_surgery_encounter_count, 0) as actual_office_visit_surgery_encounter_count
 , coalesce(enc.orphaned_claim_encounter_count, 0) as actual_orphaned_claim_encounter_count
 , coalesce(enc.outpatient_hospice_encounter_count, 0) as actual_outpatient_hospice_encounter_count
 , coalesce(enc.outpatient_hospital_or_clinic_encounter_count, 0) as actual_outpatient_hospital_or_clinic_encounter_count
 , coalesce(enc.outpatient_injections_encounter_count, 0) as actual_outpatient_injections_encounter_count
 , coalesce(enc.outpatient_psych_encounter_count, 0) as actual_outpatient_psych_encounter_count
 , coalesce(enc.outpatient_pt_ot_st_encounter_count, 0) as actual_outpatient_pt_ot_st_encounter_count
 , coalesce(enc.outpatient_radiology_encounter_count, 0) as actual_outpatient_radiology_encounter_count
 , coalesce(enc.outpatient_rehabilitation_encounter_count, 0) as actual_outpatient_rehabilitation_encounter_count
 , coalesce(enc.outpatient_substance_use_encounter_count, 0) as actual_outpatient_substance_use_encounter_count
 , coalesce(enc.outpatient_surgery_encounter_count, 0) as actual_outpatient_surgery_encounter_count
 , coalesce(enc.telehealth_encounter_count, 0) as actual_telehealth_encounter_count
 , coalesce(enc.urgent_care_encounter_count, 0) as actual_urgent_care_encounter_count

   -- ==== EXPECTED (PRED) Encounter counts (Encounter Types) =============
 , coalesce(emm.acute_inpatient_encounter_count_pred, 0) as expected_acute_inpatient_encounter_count
 , coalesce(emm.ambulance_orphaned_encounter_count_pred, 0) as expected_ambulance_orphaned_encounter_count
 , coalesce(emm.ambulatory_surgery_center_encounter_count_pred, 0) as expected_ambulatory_surgery_center_encounter_count
 , coalesce(emm.dialysis_encounter_count_pred, 0) as expected_dialysis_encounter_count
 , coalesce(emm.dme_orphaned_encounter_count_pred, 0) as expected_dme_orphaned_encounter_count
 , coalesce(emm.emergency_department_encounter_count_pred, 0) as expected_emergency_department_encounter_count
 , coalesce(emm.ed_encounter_count_pred, 0) as expected_ed_encounter_count
 , coalesce(emm.home_health_encounter_count_pred, 0) as expected_home_health_encounter_count
 , coalesce(emm.inpatient_hospice_encounter_count_pred, 0) as expected_inpatient_hospice_encounter_count
 , coalesce(emm.inpatient_long_term_acute_care_encounter_count_pred, 0) as expected_inpatient_long_term_acute_care_encounter_count
 , coalesce(emm.inpatient_psych_encounter_count_pred, 0) as expected_inpatient_psych_encounter_count
 , coalesce(emm.inpatient_rehabilitation_encounter_count_pred, 0) as expected_inpatient_rehabilitation_encounter_count
 , coalesce(emm.inpatient_skilled_nursing_encounter_count_pred, 0) as expected_inpatient_skilled_nursing_encounter_count
 , coalesce(emm.snf_encounter_count_pred, 0) as expected_snf_encounter_count
 , coalesce(emm.inpatient_substance_use_encounter_count_pred, 0) as expected_inpatient_substance_use_encounter_count
 , coalesce(emm.lab_orphaned_encounter_count_pred, 0) as expected_lab_orphaned_encounter_count
 , coalesce(emm.office_visit_encounter_count_pred, 0) as expected_office_visit_encounter_count
 , coalesce(emm.office_visit_other_encounter_count_pred, 0) as expected_office_visit_other_encounter_count
 , coalesce(emm.office_visit_injections_encounter_count_pred, 0) as expected_office_visit_injections_encounter_count
 , coalesce(emm.office_visit_pt_ot_st_encounter_count_pred, 0) as expected_office_visit_pt_ot_st_encounter_count
 , coalesce(emm.office_visit_radiology_encounter_count_pred, 0) as expected_office_visit_radiology_encounter_count
 , coalesce(emm.office_visit_surgery_encounter_count_pred, 0) as expected_office_visit_surgery_encounter_count
 , coalesce(emm.orphaned_claim_encounter_count_pred, 0) as expected_orphaned_claim_encounter_count
 , coalesce(emm.outpatient_hospice_encounter_count_pred, 0) as expected_outpatient_hospice_encounter_count
 , coalesce(emm.outpatient_hospital_or_clinic_encounter_count_pred, 0) as expected_outpatient_hospital_or_clinic_encounter_count
 , coalesce(emm.outpatient_injections_encounter_count_pred, 0) as expected_outpatient_injections_encounter_count
 , coalesce(emm.outpatient_psych_encounter_count_pred, 0) as expected_outpatient_psych_encounter_count
 , coalesce(emm.outpatient_pt_ot_st_encounter_count_pred, 0) as expected_outpatient_pt_ot_st_encounter_count
 , coalesce(emm.outpatient_radiology_encounter_count_pred, 0) as expected_outpatient_radiology_encounter_count
 , coalesce(emm.outpatient_rehabilitation_encounter_count_pred, 0) as expected_outpatient_rehabilitation_encounter_count
 , coalesce(emm.outpatient_substance_use_encounter_count_pred, 0) as expected_outpatient_substance_use_encounter_count
 , coalesce(emm.outpatient_surgery_encounter_count_pred, 0) as expected_outpatient_surgery_encounter_count
 , coalesce(emm.telehealth_encounter_count_pred, 0) as expected_telehealth_encounter_count
 , coalesce(emm.urgent_care_encounter_count_pred, 0) as expected_urgent_care_encounter_count

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