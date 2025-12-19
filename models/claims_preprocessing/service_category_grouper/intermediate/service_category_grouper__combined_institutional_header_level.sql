{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

with combine_header_models as (
  {{ dbt_utils.union_relations(
    relations=[
      ref('service_category_grouper__acute_inpatient_institutional_other'),
      ref('service_category_grouper__acute_inpatient_institutional_maternity'),
      ref('service_category_grouper__acute_inpatient_institutional_med_surg'),
      ref('service_category_grouper__inpatient_substance_use_institutional'),
      ref('service_category_grouper__ambulatory_surgery_institutional'),
      ref('service_category_grouper__dialysis_institutional'),
      ref('service_category_grouper__emergency_department_institutional'),
      ref('service_category_grouper__home_health_institutional'),
      ref('service_category_grouper__inpatient_hospice_institutional'),
      ref('service_category_grouper__outpatient_hospice_institutional'),
      ref('service_category_grouper__outpatient_hospital_or_clinic_institutional'),
      ref('service_category_grouper__outpatient_physical_therapy_institutional'),
      ref('service_category_grouper__outpatient_psychiatric_institutional'),
      ref('service_category_grouper__inpatient_skilled_nursing_institutional'),
      ref('service_category_grouper__urgent_care_institutional'),
      ref('service_category_grouper__inpatient_psychiatric_institutional'),
      ref('service_category_grouper__inpatient_rehab_institutional'),
      ref('service_category_grouper__inpatient_long_term_institutional'),
      ref('service_category_grouper__outpatient_rehab_institutional'),
      ref('service_category_grouper__outpatient_substance_use_institutional'),
      ref('service_category_grouper__outpatient_skilled_nursing_institutional'),
      ref('service_category_grouper__outpatient_surgery_institutional')
    ],
    exclude=["_loaded_at"]
  ) }}
)

select
  h.claim_id
  , h.data_source
  , h.service_category_1
  , h.service_category_2
  , h.service_category_3
  , h.source_model_name
from combine_header_models as h
