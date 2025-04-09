{{ config(
   enabled = var('financial_pmpm_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
) }}

select
    year_month
  , payer
  , {{ quote_column('plan') }}
  , data_source
  , count(1) as member_months
  , sum(total_paid) / count(1) as total_paid
  , sum(medical_paid) / count(1) as medical_paid
  , sum(inpatient_paid) / count(1) as inpatient_paid
  , sum(outpatient_paid) / count(1) as outpatient_paid
  , sum(office_based_paid) / count(1) as office_based_paid
  , sum(ancillary_paid) / count(1) as ancillary_paid
  , sum(other_paid) / count(1) as other_paid
  , sum(pharmacy_paid) / count(1) as pharmacy_paid
  , sum(acute_inpatient_paid) / count(1) as acute_inpatient_paid
  , sum(ambulance_paid) / count(1) as ambulance_paid
  , sum(ambulatory_surgery_center_paid) / count(1) as ambulatory_surgery_center_paid
  , sum(dialysis_paid) / count(1) as dialysis_paid
  , sum(durable_medical_equipment_paid) / count(1) as durable_medical_equipment_paid
  , sum(emergency_department_paid) / count(1) as emergency_department_paid
  , sum(home_health_paid) / count(1) as home_health_paid
  , sum(inpatient_hospice_paid) / count(1) as inpatient_hospice_paid
  , sum(inpatient_psychiatric_paid) / count(1) as inpatient_psychiatric_paid
  , sum(inpatient_rehabilitation_paid) / count(1) as inpatient_rehabilitation_paid
  , sum(lab_paid) / count(1) as lab_paid
  , sum(observation_paid) / count(1) as observation_paid
  , sum(office_based_other_paid) / count(1) as office_based_other_paid
  , sum(office_based_pt_ot_st_paid) / count(1) as office_based_pt_ot_st_paid
  , sum(office_based_radiology_paid) / count(1) as office_based_radiology_paid
  , sum(office_based_surgery_paid) / count(1) as office_based_surgery_paid
  , sum(office_based_visit_paid) / count(1) as office_based_visit_paid
  , sum(outpatient_hospital_or_clinic_paid) / count(1) as outpatient_hospital_or_clinic_paid
  , sum(outpatient_pt_ot_st_paid) / count(1) as outpatient_pt_ot_st_paid
  , sum(outpatient_psychiatric_paid) / count(1) as outpatient_psychiatric_paid
  , sum(outpatient_radiology_paid) / count(1) as outpatient_radiology_paid
  , sum(outpatient_rehabilitation_paid) / count(1) as outpatient_rehabilitation_paid
  , sum(outpatient_surgery_paid) / count(1) as outpatient_surgery_paid
  , sum(skilled_nursing_paid) / count(1) as skilled_nursing_paid
  , sum(telehealth_visit_paid) / count(1) as telehealth_visit_paid
  , sum(urgent_care_paid) / count(1) as urgent_care_paid
  , sum(total_allowed) / count(1) as total_allowed
  , sum(medical_allowed) / count(1) as medical_allowed
  , sum(inpatient_allowed) / count(1) as inpatient_allowed
  , sum(outpatient_allowed) / count(1) as outpatient_allowed
  , sum(office_based_allowed) / count(1) as office_based_allowed
  , sum(ancillary_allowed) / count(1) as ancillary_allowed
  , sum(other_allowed) / count(1) as other_allowed
  , sum(pharmacy_allowed) / count(1) as pharmacy_allowed
  , sum(acute_inpatient_allowed) / count(1) as acute_inpatient_allowed
  , sum(ambulance_allowed) / count(1) as ambulance_allowed
  , sum(ambulatory_surgery_center_allowed) / count(1) as ambulatory_surgery_center_allowed
  , sum(dialysis_allowed) / count(1) as dialysis_allowed
  , sum(durable_medical_equipment_allowed) / count(1) as durable_medical_equipment_allowed
  , sum(emergency_department_allowed) / count(1) as emergency_department_allowed
  , sum(home_health_allowed) / count(1) as home_health_allowed
  , sum(inpatient_hospice_allowed) / count(1) as inpatient_hospice_allowed
  , sum(inpatient_psychiatric_allowed) / count(1) as inpatient_psychiatric_allowed
  , sum(inpatient_rehabilitation_allowed) / count(1) as inpatient_rehabilitation_allowed
  , sum(lab_allowed) / count(1) as lab_allowed
  , sum(observation_allowed) / count(1) as observation_allowed
  , sum(office_based_other_allowed) / count(1) as office_based_other_allowed
  , sum(office_based_pt_ot_st_allowed) / count(1) as office_based_pt_ot_st_allowed
  , sum(office_based_radiology_allowed) / count(1) as office_based_radiology_allowed
  , sum(office_based_surgery_allowed) / count(1) as office_based_surgery_allowed
  , sum(office_based_visit_allowed) / count(1) as office_based_visit_allowed
  , sum(outpatient_hospital_or_clinic_allowed) / count(1) as outpatient_hospital_or_clinic_allowed
  , sum(outpatient_pt_ot_st_allowed) / count(1) as outpatient_pt_ot_st_allowed
  , sum(outpatient_psychiatric_allowed) / count(1) as outpatient_psychiatric_allowed
  , sum(outpatient_radiology_allowed) / count(1) as outpatient_radiology_allowed
  , sum(outpatient_rehabilitation_allowed) / count(1) as outpatient_rehabilitation_allowed
  , sum(outpatient_surgery_allowed) / count(1) as outpatient_surgery_allowed
  , sum(skilled_nursing_allowed) / count(1) as skilled_nursing_allowed
  , sum(telehealth_visit_allowed) / count(1) as telehealth_visit_allowed
  , sum(urgent_care_allowed) / count(1) as urgent_care_allowed
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('financial_pmpm__pmpm_prep') }} as a
group by
    year_month
  , payer
  , {{ quote_column('plan') }}
  , data_source
