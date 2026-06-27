{{ config(
     enabled = var('claims_enabled', False) | as_bool
   )
}}

with member_months as (
  select
      person_id
    , member_id
    , year_month
    , payer
    , {{ quote_column('plan') }}
    , payer_attributed_provider
    , payer_attributed_provider_practice
    , payer_attributed_provider_organization
    , payer_attributed_provider_lob
    , custom_attributed_provider
    , custom_attributed_provider_practice
    , custom_attributed_provider_organization
    , custom_attributed_provider_lob
    , tuva_attributed_provider
    , tuva_attributed_provider_bucket
    , tuva_attributed_provider_specialty
    , data_source
  from {{ ref('core__member_month') }}
)

, encounters as (
  select
      person_id
    , {{ year_month('encounter_start_date') }} as year_month
    , lower(cast(encounter_group as {{ dbt.type_string() }})) as encounter_group
    , lower(cast(encounter_type as {{ dbt.type_string() }})) as encounter_type
    , data_source
  from {{ ref('core__encounter') }}
  where encounter_start_date is not null
    and encounter_source_type = 'claim'
)

, utilization as (
  select
      person_id
    , year_month
    , data_source
    , count(1) as total_encounter_count

    -- encounter groups
    , sum(case when encounter_group = 'inpatient' then 1 else 0 end) as inpatient_count
    , sum(case when encounter_group = 'outpatient' then 1 else 0 end) as outpatient_count
    , sum(case when encounter_group = 'office based' then 1 else 0 end) as office_based_count
    , sum(case when encounter_group = 'other' then 1 else 0 end) as other_count

    -- encounter types
    , sum(case when encounter_type = 'acute inpatient' then 1 else 0 end) as acute_inpatient_count
    , sum(case when encounter_type = 'ambulance - orphaned' then 1 else 0 end) as ambulance_orphaned_count
    , sum(case when encounter_type = 'ambulatory surgery center' then 1 else 0 end) as ambulatory_surgery_center_count
    , sum(case when encounter_type = 'dialysis' then 1 else 0 end) as dialysis_count
    , sum(case when encounter_type = 'dme - orphaned' then 1 else 0 end) as dme_orphaned_count
    , sum(case when encounter_type = 'emergency department' then 1 else 0 end) as emergency_department_count
    , sum(case when encounter_type = 'home health' then 1 else 0 end) as home_health_count
    , sum(case when encounter_type = 'inpatient hospice' then 1 else 0 end) as inpatient_hospice_count
    , sum(case when encounter_type = 'inpatient long term acute care' then 1 else 0 end) as inpatient_long_term_acute_care_count
    , sum(case when encounter_type = 'inpatient psych' then 1 else 0 end) as inpatient_psych_count
    , sum(case when encounter_type = 'inpatient rehabilitation' then 1 else 0 end) as inpatient_rehabilitation_count
    , sum(case when encounter_type = 'inpatient skilled nursing' then 1 else 0 end) as inpatient_skilled_nursing_count
    , sum(case when encounter_type = 'inpatient substance use' then 1 else 0 end) as inpatient_substance_use_count
    , sum(case when encounter_type = 'lab - orphaned' then 1 else 0 end) as lab_orphaned_count
    , sum(case when encounter_type = 'office visit' then 1 else 0 end) as office_visit_count
    , sum(case when encounter_type = 'office visit - other' then 1 else 0 end) as office_visit_other_count
    , sum(case when encounter_type = 'office visit injections' then 1 else 0 end) as office_visit_injections_count
    , sum(case when encounter_type = 'office visit pt/ot/st' then 1 else 0 end) as office_visit_pt_ot_st_count
    , sum(case when encounter_type = 'office visit radiology' then 1 else 0 end) as office_visit_radiology_count
    , sum(case when encounter_type = 'office visit surgery' then 1 else 0 end) as office_visit_surgery_count
    , sum(case when encounter_type = 'orphaned claim' then 1 else 0 end) as orphaned_claim_count
    , sum(case when encounter_type = 'outpatient hospice' then 1 else 0 end) as outpatient_hospice_count
    , sum(case when encounter_type = 'outpatient hospital or clinic' then 1 else 0 end) as outpatient_hospital_or_clinic_count
    , sum(case when encounter_type = 'outpatient injections' then 1 else 0 end) as outpatient_injections_count
    , sum(case when encounter_type = 'outpatient psych' then 1 else 0 end) as outpatient_psych_count
    , sum(case when encounter_type = 'outpatient pt/ot/st' then 1 else 0 end) as outpatient_pt_ot_st_count
    , sum(case when encounter_type = 'outpatient radiology' then 1 else 0 end) as outpatient_radiology_count
    , sum(case when encounter_type = 'outpatient rehabilitation' then 1 else 0 end) as outpatient_rehabilitation_count
    , sum(case when encounter_type = 'outpatient substance use' then 1 else 0 end) as outpatient_substance_use_count
    , sum(case when encounter_type = 'outpatient surgery' then 1 else 0 end) as outpatient_surgery_count
    , sum(case when encounter_type = 'telehealth' then 1 else 0 end) as telehealth_count
    , sum(case when encounter_type = 'urgent care' then 1 else 0 end) as urgent_care_count
  from encounters
  group by
      person_id
    , year_month
    , data_source
)

select
    member_months.person_id
  , member_months.member_id
  , member_months.year_month
  , member_months.payer
  , member_months.{{ quote_column('plan') }}
  , member_months.payer_attributed_provider
  , member_months.payer_attributed_provider_practice
  , member_months.payer_attributed_provider_organization
  , member_months.payer_attributed_provider_lob
  , member_months.custom_attributed_provider
  , member_months.custom_attributed_provider_practice
  , member_months.custom_attributed_provider_organization
  , member_months.custom_attributed_provider_lob
  , member_months.tuva_attributed_provider
  , member_months.tuva_attributed_provider_bucket
  , member_months.tuva_attributed_provider_specialty
  , coalesce(utilization.total_encounter_count, 0) as total_encounter_count
  , coalesce(utilization.inpatient_count, 0) as inpatient_count
  , coalesce(utilization.outpatient_count, 0) as outpatient_count
  , coalesce(utilization.office_based_count, 0) as office_based_count
  , coalesce(utilization.other_count, 0) as other_count
  , coalesce(utilization.acute_inpatient_count, 0) as acute_inpatient_count
  , coalesce(utilization.ambulance_orphaned_count, 0) as ambulance_orphaned_count
  , coalesce(utilization.ambulatory_surgery_center_count, 0) as ambulatory_surgery_center_count
  , coalesce(utilization.dialysis_count, 0) as dialysis_count
  , coalesce(utilization.dme_orphaned_count, 0) as dme_orphaned_count
  , coalesce(utilization.emergency_department_count, 0) as emergency_department_count
  , coalesce(utilization.home_health_count, 0) as home_health_count
  , coalesce(utilization.inpatient_hospice_count, 0) as inpatient_hospice_count
  , coalesce(utilization.inpatient_long_term_acute_care_count, 0) as inpatient_long_term_acute_care_count
  , coalesce(utilization.inpatient_psych_count, 0) as inpatient_psych_count
  , coalesce(utilization.inpatient_rehabilitation_count, 0) as inpatient_rehabilitation_count
  , coalesce(utilization.inpatient_skilled_nursing_count, 0) as inpatient_skilled_nursing_count
  , coalesce(utilization.inpatient_substance_use_count, 0) as inpatient_substance_use_count
  , coalesce(utilization.lab_orphaned_count, 0) as lab_orphaned_count
  , coalesce(utilization.office_visit_count, 0) as office_visit_count
  , coalesce(utilization.office_visit_other_count, 0) as office_visit_other_count
  , coalesce(utilization.office_visit_injections_count, 0) as office_visit_injections_count
  , coalesce(utilization.office_visit_pt_ot_st_count, 0) as office_visit_pt_ot_st_count
  , coalesce(utilization.office_visit_radiology_count, 0) as office_visit_radiology_count
  , coalesce(utilization.office_visit_surgery_count, 0) as office_visit_surgery_count
  , coalesce(utilization.orphaned_claim_count, 0) as orphaned_claim_count
  , coalesce(utilization.outpatient_hospice_count, 0) as outpatient_hospice_count
  , coalesce(utilization.outpatient_hospital_or_clinic_count, 0) as outpatient_hospital_or_clinic_count
  , coalesce(utilization.outpatient_injections_count, 0) as outpatient_injections_count
  , coalesce(utilization.outpatient_psych_count, 0) as outpatient_psych_count
  , coalesce(utilization.outpatient_pt_ot_st_count, 0) as outpatient_pt_ot_st_count
  , coalesce(utilization.outpatient_radiology_count, 0) as outpatient_radiology_count
  , coalesce(utilization.outpatient_rehabilitation_count, 0) as outpatient_rehabilitation_count
  , coalesce(utilization.outpatient_substance_use_count, 0) as outpatient_substance_use_count
  , coalesce(utilization.outpatient_surgery_count, 0) as outpatient_surgery_count
  , coalesce(utilization.telehealth_count, 0) as telehealth_count
  , coalesce(utilization.urgent_care_count, 0) as urgent_care_count
  , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
  , member_months.data_source
from member_months
left outer join utilization
  on member_months.person_id = utilization.person_id
  and member_months.year_month = utilization.year_month
  and member_months.data_source = utilization.data_source
