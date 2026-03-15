{{ config(
     enabled = (var('semantic_layer_enabled',False) | as_bool) and (var('claims_enabled',var('tuva_marts_enabled',False)) | as_bool)
   )
}}

WITH monthly_patient_costs AS (
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
    FROM {{ ref('semantic_layer__stg_financial_pmpm__pmpm_prep') }}
),

monthly_patient_risk_cte AS (
    SELECT
        {{ year_month('collection_end_date') }} AS year_month
      , person_id
      , normalized_risk_score
    FROM {{ ref('semantic_layer__stg_cms_hcc__patient_risk_scores_monthly') }}
),

monthly_population_risk_cte AS (
    SELECT
        {{ year_month('collection_end_date') }} AS year_month
      , AVG(normalized_risk_score) AS monthly_avg_risk_score
    FROM {{ ref('semantic_layer__stg_cms_hcc__patient_risk_scores_monthly') }}
    GROUP BY
        {{ year_month('collection_end_date') }}
),
combined_data_cte AS (
    SELECT
        mm.person_id
      , mm.data_source
      , {{ concat_strings(["mm.person_id", "'|'", "mm.data_source"]) }} AS patient_source_key
      , {{ concat_strings(["mm.person_id", "'|'", "mm.year_month"]) }} as member_month_sk
      , mm.year_month
      , 1 AS member_months_value
      , mpr.normalized_risk_score
      , CASE
          WHEN pop_risk.monthly_avg_risk_score IS NOT NULL AND pop_risk.monthly_avg_risk_score != 0
          THEN mpr.normalized_risk_score / pop_risk.monthly_avg_risk_score
          ELSE NULL
        END AS population_normalized_risk_score
      , LEFT(mm.year_month, 4) AS year_nbr
      , pc.inpatient_paid
      , pc.outpatient_paid
      , pc.office_based_paid
      , pc.ancillary_paid
      , pc.other_paid
      , pc.pharmacy_paid
      , pc.acute_inpatient_paid
      , pc.ambulance_paid
      , pc.ambulatory_surgery_center_paid
      , pc.dialysis_paid
      , pc.durable_medical_equipment_paid
      , pc.emergency_department_paid
      , pc.home_health_paid
      , pc.inpatient_hospice_paid
      , pc.inpatient_psychiatric_paid
      , pc.inpatient_rehabilitation_paid
      , pc.lab_paid
      , pc.observation_paid
      , pc.office_based_other_paid
      , pc.office_based_pt_ot_st_paid
      , pc.office_based_radiology_paid
      , pc.office_based_surgery_paid
      , pc.office_based_visit_paid
      , pc.outpatient_hospice_paid
      , pc.outpatient_hospital_or_clinic_paid
      , pc.outpatient_pt_ot_st_paid
      , pc.outpatient_psychiatric_paid
      , pc.outpatient_radiology_paid
      , pc.outpatient_rehabilitation_paid
      , pc.outpatient_surgery_paid
      , pc.skilled_nursing_paid
      , pc.telehealth_visit_paid
      , pc.urgent_care_paid
      , pc.inpatient_allowed
      , pc.outpatient_allowed
      , pc.office_based_allowed
      , pc.ancillary_allowed
      , pc.other_allowed
      , pc.pharmacy_allowed
      , pc.acute_inpatient_allowed
      , pc.ambulance_allowed
      , pc.ambulatory_surgery_center_allowed
      , pc.dialysis_allowed
      , pc.durable_medical_equipment_allowed
      , pc.emergency_department_allowed
      , pc.home_health_allowed
      , pc.inpatient_hospice_allowed
      , pc.inpatient_psychiatric_allowed
      , pc.inpatient_rehabilitation_allowed
      , pc.lab_allowed
      , pc.observation_allowed
      , pc.office_based_other_allowed
      , pc.office_based_pt_ot_st_allowed
      , pc.office_based_radiology_allowed
      , pc.office_based_surgery_allowed
      , pc.office_based_visit_allowed
      , pc.outpatient_hospice_allowed
      , pc.outpatient_hospital_or_clinic_allowed
      , pc.outpatient_pt_ot_st_allowed
      , pc.outpatient_psychiatric_allowed
      , pc.outpatient_radiology_allowed
      , pc.outpatient_rehabilitation_allowed
      , pc.outpatient_surgery_allowed
      , pc.skilled_nursing_allowed
      , pc.telehealth_visit_allowed
      , pc.urgent_care_allowed
      , pc.total_paid
      , pc.medical_paid
      , pc.total_allowed
      , pc.medical_allowed
      , mm.tuva_last_run
    FROM {{ ref('semantic_layer__stg_core__member_months') }} mm
    LEFT JOIN monthly_patient_risk_cte mpr
        ON mm.person_id = mpr.person_id AND mm.year_month = mpr.year_month
    LEFT JOIN monthly_population_risk_cte pop_risk
        ON mm.year_month = pop_risk.year_month
    LEFT JOIN monthly_patient_costs pc 
        ON mm.person_id = pc.person_id AND mm.year_month = pc.year_month
)
SELECT
    cd.person_id
  , cd.year_nbr
  , cd.year_month
  , cd.member_month_sk
  , cd.member_months_value AS member_months
  , SUM(cd.member_months_value) OVER (PARTITION BY cd.person_id, cd.year_nbr) AS total_year_months
  , CASE
      WHEN SUM(cd.member_months_value) OVER (PARTITION BY cd.person_id, cd.year_nbr) > 0
      THEN CAST(cd.member_months_value AS {{ dbt.type_numeric() }}) / SUM(cd.member_months_value) OVER (PARTITION BY cd.person_id, cd.year_nbr)
      ELSE CAST(0 AS {{ dbt.type_numeric() }})
    END AS MonthAllocationFactor
  , cd.data_source
  , cd.patient_source_key
  , cd.normalized_risk_score
  , cd.inpatient_paid
  , cd.outpatient_paid
  , cd.office_based_paid
  , cd.ancillary_paid
  , cd.other_paid
  , cd.pharmacy_paid
  , cd.acute_inpatient_paid
  , cd.ambulance_paid
  , cd.ambulatory_surgery_center_paid
  , cd.dialysis_paid
  , cd.durable_medical_equipment_paid
  , cd.emergency_department_paid
  , cd.home_health_paid
  , cd.inpatient_hospice_paid
  , cd.inpatient_psychiatric_paid
  , cd.inpatient_rehabilitation_paid
  , cd.lab_paid
  , cd.observation_paid
  , cd.office_based_other_paid
  , cd.office_based_pt_ot_st_paid
  , cd.office_based_radiology_paid
  , cd.office_based_surgery_paid
  , cd.office_based_visit_paid
  , cd.outpatient_hospice_paid
  , cd.outpatient_hospital_or_clinic_paid
  , cd.outpatient_pt_ot_st_paid
  , cd.outpatient_psychiatric_paid
  , cd.outpatient_radiology_paid
  , cd.outpatient_rehabilitation_paid
  , cd.outpatient_surgery_paid
  , cd.skilled_nursing_paid
  , cd.telehealth_visit_paid
  , cd.urgent_care_paid
  , cd.inpatient_allowed
  , cd.outpatient_allowed
  , cd.office_based_allowed
  , cd.ancillary_allowed
  , cd.other_allowed
  , cd.pharmacy_allowed
  , cd.acute_inpatient_allowed
  , cd.ambulance_allowed
  , cd.ambulatory_surgery_center_allowed
  , cd.dialysis_allowed
  , cd.durable_medical_equipment_allowed
  , cd.emergency_department_allowed
  , cd.home_health_allowed
  , cd.inpatient_hospice_allowed
  , cd.inpatient_psychiatric_allowed
  , cd.inpatient_rehabilitation_allowed
  , cd.lab_allowed
  , cd.observation_allowed
  , cd.office_based_other_allowed
  , cd.office_based_pt_ot_st_allowed
  , cd.office_based_radiology_allowed
  , cd.office_based_surgery_allowed
  , cd.office_based_visit_allowed
  , cd.outpatient_hospice_allowed
  , cd.outpatient_hospital_or_clinic_allowed
  , cd.outpatient_pt_ot_st_allowed
  , cd.outpatient_psychiatric_allowed
  , cd.outpatient_radiology_allowed
  , cd.outpatient_rehabilitation_allowed
  , cd.outpatient_surgery_allowed
  , cd.skilled_nursing_allowed
  , cd.telehealth_visit_allowed
  , cd.urgent_care_allowed
  , cd.total_paid
  , cd.medical_paid
  , cd.total_allowed
  , cd.medical_allowed
  , cd.tuva_last_run
FROM combined_data_cte as cd
