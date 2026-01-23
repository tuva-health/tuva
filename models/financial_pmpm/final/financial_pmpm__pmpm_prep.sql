{{ config(
     enabled = var('financial_pmpm_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
   )
}}

with combine as (
  select
      a.person_id
    , a.member_id
    , a.year_month
    , a.payer
    , a.{{ quote_column('plan') }}
    , a.data_source
    , a.payer_attributed_provider
    , a.payer_attributed_provider_practice
    , a.payer_attributed_provider_organization
    , a.payer_attributed_provider_lob
    , a.custom_attributed_provider
    , a.custom_attributed_provider_practice
    , a.custom_attributed_provider_organization
    , a.custom_attributed_provider_lob

    -- service cat 1 paid
    , coalesce(b.inpatient_paid, 0) as inpatient_paid
    , coalesce(b.outpatient_paid, 0) as outpatient_paid
    , coalesce(b.office_based_paid, 0) as office_based_paid
    , coalesce(b.ancillary_paid, 0) as ancillary_paid
    , coalesce(b.other_paid, 0) as other_paid
    , coalesce(b.pharmacy_paid, 0) as pharmacy_paid

    -- service cat 2 paid
    , coalesce(c.acute_inpatient_paid, 0) as acute_inpatient_paid
    , coalesce(c.ambulance_paid, 0) as ambulance_paid
    , coalesce(c.ambulatory_surgery_center_paid, 0) as ambulatory_surgery_center_paid
    , coalesce(c.dialysis_paid, 0) as dialysis_paid
    , coalesce(c.durable_medical_equipment_paid, 0) as durable_medical_equipment_paid
    , coalesce(c.emergency_department_paid, 0) as emergency_department_paid
    , coalesce(c.home_health_paid, 0) as home_health_paid
    , coalesce(c.inpatient_hospice_paid, 0) as inpatient_hospice_paid
    , coalesce(c.inpatient_psychiatric_paid, 0) as inpatient_psychiatric_paid
    , coalesce(c.inpatient_rehabilitation_paid, 0) as inpatient_rehabilitation_paid
    , coalesce(c.lab_paid, 0) as lab_paid
    , coalesce(c.observation_paid, 0) as observation_paid
    , coalesce(c.office_based_other_paid, 0) as office_based_other_paid
    , coalesce(c.office_based_ptotst_paid, 0) as office_based_pt_ot_st_paid
    , coalesce(c.office_based_radiology_paid, 0) as office_based_radiology_paid
    , coalesce(c.office_based_surgery_paid, 0) as office_based_surgery_paid
    , coalesce(c.office_based_visit_paid, 0) as office_based_visit_paid
    , coalesce(c.other_paid, 0) as other_paid_2
    , coalesce(c.outpatient_hospice_paid, 0) as outpatient_hospice_paid
    , coalesce(c.outpatient_hospital_or_clinic_paid, 0) as outpatient_hospital_or_clinic_paid
    , coalesce(c.outpatient_ptotst_paid, 0) as outpatient_pt_ot_st_paid
    , coalesce(c.outpatient_psychiatric_paid, 0) as outpatient_psychiatric_paid
    , coalesce(c.outpatient_radiology_paid, 0) as outpatient_radiology_paid
    , coalesce(c.outpatient_rehabilitation_paid, 0) as outpatient_rehabilitation_paid
    , coalesce(c.outpatient_surgery_paid, 0) as outpatient_surgery_paid
    , coalesce(c.pharmacy_paid, 0) as pharmacy_paid_2
    , coalesce(c.skilled_nursing_paid, 0) as skilled_nursing_paid
    , coalesce(c.telehealth_visit_paid, 0) as telehealth_visit_paid
    , coalesce(c.urgent_care_paid, 0) as urgent_care_paid

    -- service cat 1 allowed
    , coalesce(d.inpatient_allowed, 0) as inpatient_allowed
    , coalesce(d.outpatient_allowed, 0) as outpatient_allowed
    , coalesce(d.office_based_allowed, 0) as office_based_allowed
    , coalesce(d.ancillary_allowed, 0) as ancillary_allowed
    , coalesce(d.other_allowed, 0) as other_allowed
    , coalesce(d.pharmacy_allowed, 0) as pharmacy_allowed

    -- service cat 2 allowed
    , coalesce(e.acute_inpatient_allowed, 0) as acute_inpatient_allowed
    , coalesce(e.ambulance_allowed, 0) as ambulance_allowed
    , coalesce(e.ambulatory_surgery_center_allowed, 0) as ambulatory_surgery_center_allowed
    , coalesce(e.dialysis_allowed, 0) as dialysis_allowed
    , coalesce(e.durable_medical_equipment_allowed, 0) as durable_medical_equipment_allowed
    , coalesce(e.emergency_department_allowed, 0) as emergency_department_allowed
    , coalesce(e.home_health_allowed, 0) as home_health_allowed
    , coalesce(e.inpatient_hospice_allowed, 0) as inpatient_hospice_allowed
    , coalesce(e.inpatient_psychiatric_allowed, 0) as inpatient_psychiatric_allowed
    , coalesce(e.inpatient_rehabilitation_allowed, 0) as inpatient_rehabilitation_allowed
    , coalesce(e.lab_allowed, 0) as lab_allowed
    , coalesce(e.observation_allowed, 0) as observation_allowed
    , coalesce(e.office_based_other_allowed, 0) as office_based_other_allowed
    , coalesce(e.office_based_ptotst_allowed, 0) as office_based_pt_ot_st_allowed
    , coalesce(e.office_based_radiology_allowed, 0) as office_based_radiology_allowed
    , coalesce(e.office_based_surgery_allowed, 0) as office_based_surgery_allowed
    , coalesce(e.office_based_visit_allowed, 0) as office_based_visit_allowed
    , coalesce(e.other_allowed, 0) as other_allowed_2
    , coalesce(e.outpatient_hospice_allowed, 0) as outpatient_hospice_allowed
    , coalesce(e.outpatient_hospital_or_clinic_allowed, 0) as outpatient_hospital_or_clinic_allowed
    , coalesce(e.outpatient_ptotst_allowed, 0) as outpatient_pt_ot_st_allowed
    , coalesce(e.outpatient_psychiatric_allowed, 0) as outpatient_psychiatric_allowed
    , coalesce(e.outpatient_radiology_allowed, 0) as outpatient_radiology_allowed
    , coalesce(e.outpatient_rehabilitation_allowed, 0) as outpatient_rehabilitation_allowed
    , coalesce(e.outpatient_surgery_allowed, 0) as outpatient_surgery_allowed
    , coalesce(e.pharmacy_allowed, 0) as pharmacy_allowed_2
    , coalesce(e.skilled_nursing_allowed, 0) as skilled_nursing_allowed
    , coalesce(e.telehealth_visit_allowed, 0) as telehealth_visit_allowed
    , coalesce(e.urgent_care_allowed, 0) as urgent_care_allowed

  from {{ ref('core__member_months') }} as a
  left outer join {{ ref('financial_pmpm__service_category_1_paid_pivot') }} as b
    on a.person_id = b.person_id
    and a.member_id = b.member_id
    and a.year_month = b.year_month
    and a.payer = b.payer
    and a.{{ quote_column('plan') }} = b.{{ quote_column('plan') }}
    and a.data_source = b.data_source
  left outer join {{ ref('financial_pmpm__service_category_2_paid_pivot') }} as c
    on a.person_id = c.person_id
    and a.member_id = c.member_id
    and a.year_month = c.year_month
    and a.payer = c.payer
    and a.{{ quote_column('plan') }} = c.{{ quote_column('plan') }}
    and a.data_source = c.data_source
  left outer join {{ ref('financial_pmpm__service_category_1_allowed_pivot') }} as d
    on a.person_id = d.person_id
    and a.member_id = d.member_id
    and a.year_month = d.year_month
    and a.payer = d.payer
    and a.{{ quote_column('plan') }} = d.{{ quote_column('plan') }}
    and a.data_source = d.data_source
  left outer join {{ ref('financial_pmpm__service_category_2_allowed_pivot') }} as e
    on a.person_id = e.person_id
    and a.member_id = e.member_id
    and a.year_month = e.year_month
    and a.payer = e.payer
    and a.{{ quote_column('plan') }} = e.{{ quote_column('plan') }}
    and a.data_source = e.data_source
)

select
    *
  , inpatient_paid + outpatient_paid + office_based_paid + ancillary_paid + other_paid + pharmacy_paid as total_paid
  , inpatient_paid + outpatient_paid + office_based_paid + ancillary_paid + other_paid as medical_paid
  , inpatient_allowed + outpatient_allowed + office_based_allowed + ancillary_allowed + other_allowed + pharmacy_allowed as total_allowed
  , inpatient_allowed + outpatient_allowed + office_based_allowed + ancillary_allowed + other_allowed as medical_allowed
  , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from combine
