{{ config(
     enabled = (var('semantic_layer_enabled',False) | as_bool) and (var('claims_enabled',var('tuva_marts_enabled',False)) | as_bool)
   )
}}

SELECT
        person_id
      , year_month
      , data_source
      , inpatient_paid
      , outpatient_paid
      , office_based_paid
      , ancillary_paid
      , other_paid
      , pharmacy_paid
      , acute_inpatient_paid
      , ambulance_paid
      , ambulatory_surgery_center_paid
      , dialysis_paid
      , durable_medical_equipment_paid
      , emergency_department_paid
      , home_health_paid
      , inpatient_hospice_paid
      , inpatient_psychiatric_paid
      , inpatient_rehabilitation_paid
      , lab_paid
      , observation_paid
      , office_based_other_paid
      , office_based_pt_ot_st_paid
      , office_based_radiology_paid
      , office_based_surgery_paid
      , office_based_visit_paid
      , outpatient_hospice_paid
      , outpatient_hospital_or_clinic_paid
      , outpatient_pt_ot_st_paid
      , outpatient_psychiatric_paid
      , outpatient_radiology_paid
      , outpatient_rehabilitation_paid
      , outpatient_surgery_paid
      , skilled_nursing_paid
      , telehealth_visit_paid
      , urgent_care_paid
      , inpatient_allowed
      , outpatient_allowed
      , office_based_allowed
      , ancillary_allowed
      , other_allowed
      , pharmacy_allowed
      , acute_inpatient_allowed
      , ambulance_allowed
      , ambulatory_surgery_center_allowed
      , dialysis_allowed
      , durable_medical_equipment_allowed
      , emergency_department_allowed
      , home_health_allowed
      , inpatient_hospice_allowed
      , inpatient_psychiatric_allowed
      , inpatient_rehabilitation_allowed
      , lab_allowed
      , observation_allowed
      , office_based_other_allowed
      , office_based_pt_ot_st_allowed
      , office_based_radiology_allowed
      , office_based_surgery_allowed
      , office_based_visit_allowed
      , outpatient_hospice_allowed
      , outpatient_hospital_or_clinic_allowed
      , outpatient_pt_ot_st_allowed
      , outpatient_psychiatric_allowed
      , outpatient_radiology_allowed
      , outpatient_rehabilitation_allowed
      , outpatient_surgery_allowed
      , skilled_nursing_allowed
      , telehealth_visit_allowed
      , urgent_care_allowed
      , total_paid
      , medical_paid
      , total_allowed
      , medical_allowed
      , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
    FROM {{ ref('financial_pmpm__pmpm_prep') }}