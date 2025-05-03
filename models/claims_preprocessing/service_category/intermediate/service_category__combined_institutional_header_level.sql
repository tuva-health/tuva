{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

with combine_header_models as (
  {{ dbt_utils.union_relations(
    relations=[
      ref('service_category__acute_inpatient_institutional_other'),
      ref('service_category__acute_inpatient_institutional_maternity'),
      ref('service_category__acute_inpatient_institutional_med_surg'),
      ref('service_category__inpatient_substance_use_institutional'),
      ref('service_category__ambulatory_surgery_institutional'),
      ref('service_category__dialysis_institutional'),
      ref('service_category__emergency_department_institutional'),
      ref('service_category__home_health_institutional'),
      ref('service_category__inpatient_hospice_institutional'),
      ref('service_category__outpatient_hospice_institutional'),
      ref('service_category__outpatient_hospital_or_clinic_institutional'),
      ref('service_category__outpatient_physical_therapy_institutional'),
      ref('service_category__outpatient_psychiatric_institutional'),
      ref('service_category__inpatient_skilled_nursing_institutional'),
      ref('service_category__urgent_care_institutional'),
      ref('service_category__inpatient_psychiatric_institutional'),
      ref('service_category__inpatient_rehab_institutional'),
      ref('service_category__outpatient_rehab_institutional'),
      ref('service_category__outpatient_substance_use_institutional'),
      ref('service_category__outpatient_skilled_nursing_institutional'),
      ref('service_category__outpatient_surgery_institutional')
    ],
    exclude=["_loaded_at"]
  ) }}
)

select
  h.claim_id
  , h.service_category_1
  , h.service_category_2
  , h.service_category_3
  , h.source_model_name
from combine_header_models as h
