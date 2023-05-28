{{ config(
     enabled = var('pmpm_enabled',var('tuva_marts_enabled',True))
   )
}}

with service_cat_2 as (
select
  patient_id
, year_month
, service_category_2
, sum(total_paid) as total_paid
from {{ ref('pmpm__patient_spend_with_service_categories') }}
group by 1,2,3
)

select
patient_id, 
year_month, 
 {{ dbt_utils.pivot(
      column='service_category_2'
    , values=('Acute Inpatient',
              'Ambulance',
              'Ambulatory Surgery',
              'Dialysis',
              'Durable Medical Equipment',
              'Emergency Department',
              'Home Health',
              'Hospice',
              'Inpatient Psychiatric',
              'Inpatient Rehabilitation',
              'Lab',
              'Office Visit',
              'Outpatient Hospital or Clinic',
              'Outpatient Psychiatric',
              'Outpatient Rehabilitation',
              'Skilled Nursing',
              'Urgent Care'                                                 
              )
    , agg='sum'
    , then_value='total_paid'
    , else_value= 0
    , quote_identifiers = False
    , suffix='_paid'
  ) }}
-- acute_inpatient_paid, 
-- ambulance_paid, 
-- ambulatory_surgery_paid, 
-- dialysis_paid, 
-- durable_medical_equipment_paid, 
-- emergency_department_paid,
-- home_health_paid, 
-- hospice_paid, 
-- inpatient_psychiatric_paid, 
-- inpatient_rehabilitation_paid, 
-- lab_paid,
-- office_visit_paid, 
-- outpatient_hospital_or_clinic_paid, 
-- outpatient_psychiatric_paid, 
-- outpatient_rehabilitation_paid,
-- skilled_nursing_paid, 
-- urgent_care_paid
from service_cat_2
-- pivot(sum(total_paid) for service_category_2 in ('Acute Inpatient',
--                                                  'Ambulance',
--                                                  'Ambulatory Surgery',
--                                                  'Dialysis',
--                                                  'Durable Medical Equipment',
--                                                  'Emergency Department',
--                                                  'Home Health',
--                                                  'Hospice',
--                                                  'Inpatient Psychiatric',
--                                                  'Inpatient Rehabilitation',
--                                                  'Lab',
--                                                  'Office Visit',
--                                                  'Outpatient Hospital or Clinic',
--                                                  'Outpatient Psychiatric',
--                                                  'Outpatient Rehabilitation',
--                                                  'Skilled Nursing',
--                                                  'Urgent Care'                                                 
--                                                  )) 
--   as p (patient_id, 
--         year_month, 
--         acute_inpatient_paid, 
--         ambulance_paid, 
--         ambulatory_surgery_paid, 
--         dialysis_paid, 
--         durable_medical_equipment_paid, 
--         emergency_department_paid,
--         home_health_paid, 
--         hospice_paid, 
--         inpatient_psychiatric_paid, 
--         inpatient_rehabilitation_paid, 
--         lab_paid,
--         office_visit_paid, 
--         outpatient_hospital_or_clinic_paid, 
--         outpatient_psychiatric_paid, 
--         outpatient_rehabilitation_paid,
--         skilled_nursing_paid, 
--         urgent_care_paid
--         )
group by 1,2