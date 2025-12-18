{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

with combined_professional_services as (
  {{ dbt_utils.union_relations(
    relations=[
      ref('service_category_grouper__acute_inpatient_professional'),
      ref('service_category_grouper__ambulatory_surgery_professional'),
      ref('service_category_grouper__dialysis_professional'),
      ref('service_category_grouper__emergency_department_professional'),
      ref('service_category_grouper__home_health_professional'),
      ref('service_category_grouper__inpatient_hospice_professional'),
      ref('service_category_grouper__inpatient_psychiatric_professional'),
      ref('service_category_grouper__inpatient_rehab_professional'),
      ref('service_category_grouper__inpatient_substance_use_professional'),
      ref('service_category_grouper__lab_professional'),
      ref('service_category_grouper__office_based_other_professional'),
      ref('service_category_grouper__office_based_physical_therapy_professional'),
      ref('service_category_grouper__office_based_radiology'),
      ref('service_category_grouper__office_based_surgery_professional'),
      ref('service_category_grouper__office_based_visit_professional'),
      ref('service_category_grouper__outpatient_hospital_or_clinic_professional'),
      ref('service_category_grouper__outpatient_psychiatric_professional'),
      ref('service_category_grouper__outpatient_rehab_professional'),
      ref('service_category_grouper__inpatient_skilled_nursing_professional'),
      ref('service_category_grouper__urgent_care_professional'),
      ref('service_category_grouper__outpatient_hospice_professional'),
      ref('service_category_grouper__pharmacy_professional'),
      ref('service_category_grouper__outpatient_substance_use_professional'),
      ref('service_category_grouper__outpatient_physical_therapy_professional'),
      ref('service_category_grouper__outpatient_radiology_professional'),
      ref('service_category_grouper__observation_professional'),
      ref('service_category_grouper__dme_professional'),
      ref('service_category_grouper__ambulance_professional'),
      ref('service_category_grouper__outpatient_surgery_professional')
    ],
    exclude=["_loaded_at"]
  ) }}
)

select
    p.claim_id
    , p.claim_line_number
    , p.data_source
    , p.claim_line_id
    , p.service_category_1
    , p.service_category_2
    , p.service_category_3
    , p.tuva_last_run
    , p.source_model_name
from combined_professional_services as p
