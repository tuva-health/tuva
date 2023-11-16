{{ config(
     enabled = var('financial_pmpm_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

with combine as (
SELECT
  a.patient_id,
  a.year_month,
  a.payer,
  a.plan, 
  a.data_source,
  
  -- service cat 1 paid
  COALESCE(b.inpatient_paid, 0) AS inpatient_paid,
  COALESCE(b.outpatient_paid, 0) AS outpatient_paid,
  COALESCE(b.office_visit_paid, 0) AS office_visit_paid,
  COALESCE(b.ancillary_paid, 0) AS ancillary_paid,
  COALESCE(b.pharmacy_paid, 0) AS pharmacy_paid,
  COALESCE(b.other_paid, 0) AS other_paid,
  
  -- service cat 2 paid
  COALESCE(c.acute_inpatient_paid, 0) AS acute_inpatient_paid,
  COALESCE(c.ambulance_paid, 0) AS ambulance_paid,
  COALESCE(c.ambulatory_surgery_paid, 0) AS ambulatory_surgery_paid,
  COALESCE(c.dialysis_paid, 0) AS dialysis_paid,
  COALESCE(c.durable_medical_equipment_paid, 0) AS durable_medical_equipment_paid,
  COALESCE(c.emergency_department_paid, 0) AS emergency_department_paid,
  COALESCE(c.home_health_paid, 0) AS home_health_paid,
  COALESCE(c.hospice_paid, 0) AS hospice_paid,
  COALESCE(c.inpatient_psychiatric_paid, 0) AS inpatient_psychiatric_paid,
  COALESCE(c.inpatient_rehabilitation_paid, 0) AS inpatient_rehabilitation_paid,
  COALESCE(c.lab_paid, 0) AS lab_paid,
  COALESCE(c.office_visit_paid, 0) AS office_visit_paid_2,
  COALESCE(c.outpatient_hospital_or_clinic_paid, 0) AS outpatient_hospital_or_clinic_paid,
  COALESCE(c.outpatient_psychiatric_paid, 0) AS outpatient_psychiatric_paid,
  COALESCE(c.outpatient_rehabilitation_paid, 0) AS outpatient_rehabilitation_paid,
  COALESCE(c.skilled_nursing_paid, 0) AS skilled_nursing_paid,
  COALESCE(c.urgent_care_paid, 0) AS urgent_care_paid,
  
  -- service cat 1 allowed
  COALESCE(d.inpatient_allowed, 0) AS inpatient_allowed,
  COALESCE(d.outpatient_allowed, 0) AS outpatient_allowed,
  COALESCE(d.office_visit_allowed, 0) AS office_visit_allowed,
  COALESCE(d.ancillary_allowed, 0) AS ancillary_allowed,
  COALESCE(d.pharmacy_allowed, 0) AS pharmacy_allowed,
  COALESCE(d.other_allowed, 0) AS other_allowed,
  
  -- service cat 2 allowed
  COALESCE(e.acute_inpatient_allowed, 0) AS acute_inpatient_allowed,
  COALESCE(e.ambulance_allowed, 0) AS ambulance_allowed,
  COALESCE(e.ambulatory_surgery_allowed, 0) AS ambulatory_surgery_allowed,
  COALESCE(e.dialysis_allowed, 0) AS dialysis_allowed,
  COALESCE(e.durable_medical_equipment_allowed, 0) AS durable_medical_equipment_allowed,
  COALESCE(e.emergency_department_allowed, 0) AS emergency_department_allowed,
  COALESCE(e.home_health_allowed, 0) AS home_health_allowed,
  COALESCE(e.hospice_allowed, 0) AS hospice_allowed,
  COALESCE(e.inpatient_psychiatric_allowed, 0) AS inpatient_psychiatric_allowed,
  COALESCE(e.inpatient_rehabilitation_allowed, 0) AS inpatient_rehabilitation_allowed,
  COALESCE(e.lab_allowed, 0) AS lab_allowed,
  COALESCE(e.office_visit_allowed, 0) AS office_visit_allowed_2,
  COALESCE(e.outpatient_hospital_or_clinic_allowed, 0) AS outpatient_hospital_or_clinic_allowed,
  COALESCE(e.outpatient_psychiatric_allowed, 0) AS outpatient_psychiatric_allowed,
  COALESCE(e.outpatient_rehabilitation_allowed, 0) AS outpatient_rehabilitation_allowed,
  COALESCE(e.skilled_nursing_allowed, 0) AS skilled_nursing_allowed,
  COALESCE(e.urgent_care_allowed, 0) AS urgent_care_allowed
FROM {{ ref('financial_pmpm__member_months') }} a
left join {{ ref('financial_pmpm__service_category_1_paid_pivot') }} b
  on a.patient_id = b.patient_id
  and a.year_month = b.year_month
  and a.payer = b.payer
  and a.plan = b.plan
left join {{ ref('financial_pmpm__service_category_2_paid_pivot') }} c
  on a.patient_id = c.patient_id
  and a.year_month = c.year_month
  and a.payer = c.payer
  and a.plan = c.plan
left join {{ ref('financial_pmpm__service_category_1_allowed_pivot') }} d
  on a.patient_id = d.patient_id
  and a.year_month = d.year_month
  and a.payer = d.payer
  and a.plan = d.plan
left join {{ ref('financial_pmpm__service_category_2_allowed_pivot') }} e
  on a.patient_id = e.patient_id
  and a.year_month = e.year_month
  and a.payer = e.payer
  and a.plan = e.plan   
)

select *
, inpatient_paid + outpatient_paid + office_visit_paid + ancillary_paid + other_paid + pharmacy_paid as total_paid
, inpatient_paid + outpatient_paid + office_visit_paid + ancillary_paid + other_paid as medical_paid
, inpatient_allowed + outpatient_allowed + office_visit_allowed + ancillary_allowed + other_allowed + pharmacy_allowed as total_allowed
, inpatient_allowed + outpatient_allowed + office_visit_allowed + ancillary_allowed + other_allowed as medical_allowed
, '{{ var('tuva_last_run')}}' as tuva_last_run
from combine