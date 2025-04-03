{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

WITH combined_professional_services AS (
  {{ dbt_utils.union_relations(
    relations=[
      ref('service_category__acute_inpatient_professional'),
      ref('service_category__ambulatory_surgery_professional'),
      ref('service_category__dialysis_professional'),
      ref('service_category__emergency_department_professional'),
      ref('service_category__home_health_professional'),
      ref('service_category__inpatient_hospice_professional'),
      ref('service_category__inpatient_psychiatric_professional'),
      ref('service_category__inpatient_rehab_professional'),
      ref('service_category__inpatient_substance_use_professional'),
      ref('service_category__lab_professional'),
      ref('service_category__office_based_other_professional'),
      ref('service_category__office_based_physical_therapy_professional'),
      ref('service_category__office_based_radiology'),
      ref('service_category__office_based_surgery_professional'),
      ref('service_category__office_based_visit_professional'),
      ref('service_category__outpatient_hospital_or_clinic_professional'),
      ref('service_category__outpatient_psychiatric_professional'),
      ref('service_category__outpatient_rehab_professional'),
      ref('service_category__inpatient_skilled_nursing_professional'),
      ref('service_category__urgent_care_professional'),
      ref('service_category__outpatient_hospice_professional'),
      ref('service_category__pharmacy_professional'),
      ref('service_category__outpatient_substance_use_professional'),
      ref('service_category__outpatient_physical_therapy_professional'),
      ref('service_category__outpatient_radiology_professional'),
      ref('service_category__observation_professional'),
      ref('service_category__dme_professional'),
      ref('service_category__ambulance_professional'),
      ref('service_category__outpatient_surgery_professional')
    ],
    exclude=["_loaded_at"]
  ) }}
)

SELECT
    p.claim_id,
    p.claim_line_number,
    p.claim_line_id,
    p.service_category_1,
    p.service_category_2,
    p.service_category_3,
    p.tuva_last_run,
    p.source_model_name
FROM combined_professional_services p
