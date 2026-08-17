{{ config(
     enabled = (var('parity_enabled', false) | as_bool)
       and (var('claims_enabled', false) | as_bool)
   )
}}

{#
  This list is the immutable parity metric catalog.

  - Keep metric IDs as quoted, zero-padded strings.
  - Never renumber, reuse, or redefine an existing metric ID.
  - Append a new ID when adding a metric or changing a metric definition.
  - Keep the catalog fixed in code; do not generate IDs from values in the data.
#}
{% set metric_catalog = [
    ('0001', 'Patients from claims data', 'patient_metrics', 'patient_count'),
    ('0002', 'Female patients from claims data', 'patient_metrics', 'female_patient_count'),
    ('0003', 'Male patients from claims data', 'patient_metrics', 'male_patient_count'),
    ('0004', 'Patients from claims data with unknown or other sex', 'patient_metrics', 'unclassified_sex_patient_count'),
    ('0005', 'Eligibility records', 'eligibility_metrics', 'eligibility_record_count'),
    ('0006', 'People with eligibility', 'eligibility_metrics', 'eligible_person_count'),
    ('0007', 'Covered person-member-plan combinations', 'eligibility_metrics', 'covered_member_plan_count'),
    ('0008', 'Core member months', 'member_month_metrics', 'member_month_count'),
    ('0009', 'People with core member months', 'member_month_metrics', 'member_month_person_count'),
    ('0010', 'Enrolled medical claim lines', 'medical_claim_metrics', 'enrolled_claim_line_count'),
    ('0011', 'Unenrolled medical claim lines', 'medical_claim_metrics', 'unenrolled_claim_line_count'),
    ('0012', 'Enrolled pharmacy claim lines', 'pharmacy_claim_metrics', 'enrolled_claim_line_count'),
    ('0013', 'Unenrolled pharmacy claim lines', 'pharmacy_claim_metrics', 'unenrolled_claim_line_count'),
    ('0014', 'Medical claim lines', 'medical_claim_metrics', 'claim_line_count'),
    ('0015', 'Distinct medical claims', 'medical_claim_metrics', 'claim_count'),
    ('0016', 'Institutional medical claim lines', 'medical_claim_metrics', 'institutional_claim_line_count'),
    ('0017', 'Professional medical claim lines', 'medical_claim_metrics', 'professional_claim_line_count'),
    ('0018', 'Medical claim lines with unclassified claim type', 'medical_claim_metrics', 'unclassified_claim_type_line_count'),
    ('0019', 'Medical claim paid amount', 'medical_claim_metrics', 'paid_amount'),
    ('0020', 'Medical claim allowed amount', 'medical_claim_metrics', 'allowed_amount'),
    ('0021', 'Medical claim charge amount', 'medical_claim_metrics', 'charge_amount'),
    ('0022', 'Medical claim total cost amount', 'medical_claim_metrics', 'total_cost_amount'),
    ('0023', 'Pharmacy claim lines', 'pharmacy_claim_metrics', 'claim_line_count'),
    ('0024', 'Distinct pharmacy claims', 'pharmacy_claim_metrics', 'claim_count'),
    ('0025', 'Pharmacy claim paid amount', 'pharmacy_claim_metrics', 'paid_amount'),
    ('0026', 'Pharmacy claim allowed amount', 'pharmacy_claim_metrics', 'allowed_amount'),
    ('0027', 'Pharmacy claim charge amount', 'pharmacy_claim_metrics', 'charge_amount'),
    ('0028', 'Medical plus pharmacy claim paid amount', 'combined_claim_metrics', 'paid_amount'),
    ('0029', 'Medical plus pharmacy claim allowed amount', 'combined_claim_metrics', 'allowed_amount'),
    ('0030', 'Inpatient medical claim lines', 'medical_claim_metrics', 'inpatient_claim_line_count'),
    ('0031', 'Outpatient medical claim lines', 'medical_claim_metrics', 'outpatient_claim_line_count'),
    ('0032', 'Office-based medical claim lines', 'medical_claim_metrics', 'office_based_claim_line_count'),
    ('0033', 'Ancillary medical claim lines', 'medical_claim_metrics', 'ancillary_claim_line_count'),
    ('0034', 'Medical claim lines with pharmacy service category', 'medical_claim_metrics', 'pharmacy_claim_line_count'),
    ('0035', 'Other medical claim lines', 'medical_claim_metrics', 'other_claim_line_count'),
    ('0036', 'Medical claim lines with unclassified service category 1', 'medical_claim_metrics', 'unclassified_service_category_1_line_count'),
    ('0037', 'Inpatient medical claim paid amount', 'medical_claim_metrics', 'inpatient_paid_amount'),
    ('0038', 'Inpatient medical claim allowed amount', 'medical_claim_metrics', 'inpatient_allowed_amount'),
    ('0039', 'Outpatient medical claim paid amount', 'medical_claim_metrics', 'outpatient_paid_amount'),
    ('0040', 'Outpatient medical claim allowed amount', 'medical_claim_metrics', 'outpatient_allowed_amount'),
    ('0041', 'Office-based medical claim paid amount', 'medical_claim_metrics', 'office_based_paid_amount'),
    ('0042', 'Office-based medical claim allowed amount', 'medical_claim_metrics', 'office_based_allowed_amount'),
    ('0043', 'Ancillary medical claim paid amount', 'medical_claim_metrics', 'ancillary_paid_amount'),
    ('0044', 'Ancillary medical claim allowed amount', 'medical_claim_metrics', 'ancillary_allowed_amount'),
    ('0045', 'Medical claim paid amount with pharmacy service category', 'medical_claim_metrics', 'pharmacy_paid_amount'),
    ('0046', 'Medical claim allowed amount with pharmacy service category', 'medical_claim_metrics', 'pharmacy_allowed_amount'),
    ('0047', 'Other medical claim paid amount', 'medical_claim_metrics', 'other_paid_amount'),
    ('0048', 'Other medical claim allowed amount', 'medical_claim_metrics', 'other_allowed_amount'),
    ('0049', 'Unclassified medical claim paid amount', 'medical_claim_metrics', 'unclassified_paid_amount'),
    ('0050', 'Unclassified medical claim allowed amount', 'medical_claim_metrics', 'unclassified_allowed_amount'),
    ('0051', 'Claim encounters', 'encounter_metrics', 'encounter_count'),
    ('0052', 'Person-data-source combinations with claim encounters', 'encounter_metrics', 'encounter_person_count'),
    ('0053', 'Claim encounter paid amount', 'encounter_metrics', 'paid_amount'),
    ('0054', 'Claim encounter allowed amount', 'encounter_metrics', 'allowed_amount'),
    ('0055', 'Claim encounter charge amount', 'encounter_metrics', 'charge_amount'),
    ('0056', 'Sum of claim counts across claim encounters', 'encounter_metrics', 'linked_claim_count'),
    ('0057', 'Inpatient claim encounters', 'encounter_metrics', 'inpatient_encounter_count'),
    ('0058', 'Outpatient claim encounters', 'encounter_metrics', 'outpatient_encounter_count'),
    ('0059', 'Office-based claim encounters', 'encounter_metrics', 'office_based_encounter_count'),
    ('0060', 'Other claim encounters', 'encounter_metrics', 'other_encounter_count'),
    ('0061', 'Claim encounters with unclassified encounter group', 'encounter_metrics', 'unclassified_encounter_group_count'),
    ('0062', 'Acute inpatient encounters', 'encounter_metrics', 'acute_inpatient_count'),
    ('0063', 'Ambulance orphaned encounters', 'encounter_metrics', 'ambulance_orphaned_count'),
    ('0064', 'Ambulatory surgery center encounters', 'encounter_metrics', 'ambulatory_surgery_center_count'),
    ('0065', 'Dialysis encounters', 'encounter_metrics', 'dialysis_count'),
    ('0066', 'DME orphaned encounters', 'encounter_metrics', 'dme_orphaned_count'),
    ('0067', 'Emergency department encounters', 'encounter_metrics', 'emergency_department_count'),
    ('0068', 'Home health encounters', 'encounter_metrics', 'home_health_count'),
    ('0069', 'Inpatient hospice encounters', 'encounter_metrics', 'inpatient_hospice_count'),
    ('0070', 'Inpatient long term acute care encounters', 'encounter_metrics', 'inpatient_long_term_acute_care_count'),
    ('0071', 'Inpatient psych encounters', 'encounter_metrics', 'inpatient_psych_count'),
    ('0072', 'Inpatient rehabilitation encounters', 'encounter_metrics', 'inpatient_rehabilitation_count'),
    ('0073', 'Inpatient skilled nursing encounters', 'encounter_metrics', 'inpatient_skilled_nursing_count'),
    ('0074', 'Inpatient substance use encounters', 'encounter_metrics', 'inpatient_substance_use_count'),
    ('0075', 'Lab orphaned encounters', 'encounter_metrics', 'lab_orphaned_count'),
    ('0076', 'Office visit encounters', 'encounter_metrics', 'office_visit_count'),
    ('0077', 'Office visit other encounters', 'encounter_metrics', 'office_visit_other_count'),
    ('0078', 'Office visit injection encounters', 'encounter_metrics', 'office_visit_injections_count'),
    ('0079', 'Office visit PT OT ST encounters', 'encounter_metrics', 'office_visit_pt_ot_st_count'),
    ('0080', 'Office visit radiology encounters', 'encounter_metrics', 'office_visit_radiology_count'),
    ('0081', 'Office visit surgery encounters', 'encounter_metrics', 'office_visit_surgery_count'),
    ('0082', 'Orphaned claim encounters', 'encounter_metrics', 'orphaned_claim_count'),
    ('0083', 'Outpatient hospice encounters', 'encounter_metrics', 'outpatient_hospice_count'),
    ('0084', 'Outpatient hospital or clinic encounters', 'encounter_metrics', 'outpatient_hospital_or_clinic_count'),
    ('0085', 'Outpatient injection encounters', 'encounter_metrics', 'outpatient_injections_count'),
    ('0086', 'Outpatient psych encounters', 'encounter_metrics', 'outpatient_psych_count'),
    ('0087', 'Outpatient PT OT ST encounters', 'encounter_metrics', 'outpatient_pt_ot_st_count'),
    ('0088', 'Outpatient radiology encounters', 'encounter_metrics', 'outpatient_radiology_count'),
    ('0089', 'Outpatient rehabilitation encounters', 'encounter_metrics', 'outpatient_rehabilitation_count'),
    ('0090', 'Outpatient substance use encounters', 'encounter_metrics', 'outpatient_substance_use_count'),
    ('0091', 'Outpatient surgery encounters', 'encounter_metrics', 'outpatient_surgery_count'),
    ('0092', 'Telehealth encounters', 'encounter_metrics', 'telehealth_count'),
    ('0093', 'Urgent care encounters', 'encounter_metrics', 'urgent_care_count'),
    ('0094', 'Claim encounters with unclassified encounter type', 'encounter_metrics', 'unclassified_encounter_type_count'),
    ('0095', 'Claims-derived conditions', 'condition_metrics', 'condition_count'),
    ('0096', 'Claims-derived procedures', 'procedure_metrics', 'procedure_count'),
    ('0097', 'Claims Preprocessing member months', 'claims_preprocessing_metrics', 'member_month_count'),
    ('0098', 'Service category candidate assignments', 'claims_preprocessing_metrics', 'service_category_candidate_count'),
    ('0099', 'Primary service category assignments', 'claims_preprocessing_metrics', 'primary_service_category_count'),
    ('0100', 'Medical claim enrollment matches', 'claims_preprocessing_metrics', 'medical_enrollment_match_count'),
    ('0101', 'Pharmacy claim enrollment matches', 'claims_preprocessing_metrics', 'pharmacy_enrollment_match_count')
] %}

with eligible_people as (
    select distinct
        person_id
        , data_source
    from {{ ref('core__eligibility') }}
    where person_id is not null
)

, eligible_member_plans as (
    select distinct
        person_id
        , member_id
        , payer
        , {{ quote_column('plan') }}
        , data_source
    from {{ ref('core__eligibility') }}
    where member_id is not null
)

, claims_patients as (
    select
        patient.person_id
        , patient.data_source
        , lower(trim(cast(patient.sex as {{ dbt.type_string() }}))) as sex
    from {{ ref('core__patient') }} as patient
    inner join eligible_people
        on patient.person_id = eligible_people.person_id
        and patient.data_source = eligible_people.data_source
)

, patient_metrics as (
    select
        count(*) as patient_count
        , coalesce(sum(case when sex = 'female' then 1 else 0 end), 0) as female_patient_count
        , coalesce(sum(case when sex = 'male' then 1 else 0 end), 0) as male_patient_count
        , coalesce(sum(case when sex is null or sex not in ('female', 'male') then 1 else 0 end), 0)
            as unclassified_sex_patient_count
    from claims_patients
)

, eligibility_metrics as (
    select
        (select count(*) from {{ ref('core__eligibility') }}) as eligibility_record_count
        , (select count(*) from eligible_people) as eligible_person_count
        , (select count(*) from eligible_member_plans) as covered_member_plan_count
)

, member_month_people as (
    select distinct
        person_id
        , data_source
    from {{ ref('core__member_month') }}
    where person_id is not null
)

, member_month_metrics as (
    select
        (select count(*) from {{ ref('core__member_month') }}) as member_month_count
        , (select count(*) from member_month_people) as member_month_person_count
)

, medical_claims as (
    select
        claim_id
        , data_source
        , lower(trim(cast(claim_type as {{ dbt.type_string() }}))) as claim_type
        , lower(trim(cast(service_category_1 as {{ dbt.type_string() }}))) as service_category_1
        , enrollment_flag
        , paid_amount
        , allowed_amount
        , charge_amount
        , total_cost_amount
    from {{ ref('core__medical_claim') }}
)

, distinct_medical_claims as (
    select distinct
        claim_id
        , data_source
    from medical_claims
    where claim_id is not null
)

, medical_claim_metrics as (
    select
        count(*) as claim_line_count
        , (select count(*) from distinct_medical_claims) as claim_count
        , coalesce(sum(case when coalesce(enrollment_flag, 0) = 1 then 1 else 0 end), 0)
            as enrolled_claim_line_count
        , coalesce(sum(case when coalesce(enrollment_flag, 0) <> 1 then 1 else 0 end), 0)
            as unenrolled_claim_line_count
        , coalesce(sum(case when claim_type = 'institutional' then 1 else 0 end), 0)
            as institutional_claim_line_count
        , coalesce(sum(case when claim_type = 'professional' then 1 else 0 end), 0)
            as professional_claim_line_count
        , coalesce(sum(case
            when claim_type is null or claim_type not in ('institutional', 'professional') then 1
            else 0
          end), 0) as unclassified_claim_type_line_count
        , coalesce(sum(paid_amount), 0) as paid_amount
        , coalesce(sum(allowed_amount), 0) as allowed_amount
        , coalesce(sum(charge_amount), 0) as charge_amount
        , coalesce(sum(total_cost_amount), 0) as total_cost_amount
        , coalesce(sum(case when service_category_1 = 'inpatient' then 1 else 0 end), 0)
            as inpatient_claim_line_count
        , coalesce(sum(case when service_category_1 = 'outpatient' then 1 else 0 end), 0)
            as outpatient_claim_line_count
        , coalesce(sum(case when service_category_1 = 'office-based' then 1 else 0 end), 0)
            as office_based_claim_line_count
        , coalesce(sum(case when service_category_1 = 'ancillary' then 1 else 0 end), 0)
            as ancillary_claim_line_count
        , coalesce(sum(case when service_category_1 = 'pharmacy' then 1 else 0 end), 0)
            as pharmacy_claim_line_count
        , coalesce(sum(case when service_category_1 = 'other' then 1 else 0 end), 0)
            as other_claim_line_count
        , coalesce(sum(case
            when service_category_1 is null
              or service_category_1 not in ('inpatient', 'outpatient', 'office-based', 'ancillary', 'pharmacy', 'other') then 1
            else 0
          end), 0) as unclassified_service_category_1_line_count
        , coalesce(sum(case when service_category_1 = 'inpatient' then paid_amount else 0 end), 0)
            as inpatient_paid_amount
        , coalesce(sum(case when service_category_1 = 'inpatient' then allowed_amount else 0 end), 0)
            as inpatient_allowed_amount
        , coalesce(sum(case when service_category_1 = 'outpatient' then paid_amount else 0 end), 0)
            as outpatient_paid_amount
        , coalesce(sum(case when service_category_1 = 'outpatient' then allowed_amount else 0 end), 0)
            as outpatient_allowed_amount
        , coalesce(sum(case when service_category_1 = 'office-based' then paid_amount else 0 end), 0)
            as office_based_paid_amount
        , coalesce(sum(case when service_category_1 = 'office-based' then allowed_amount else 0 end), 0)
            as office_based_allowed_amount
        , coalesce(sum(case when service_category_1 = 'ancillary' then paid_amount else 0 end), 0)
            as ancillary_paid_amount
        , coalesce(sum(case when service_category_1 = 'ancillary' then allowed_amount else 0 end), 0)
            as ancillary_allowed_amount
        , coalesce(sum(case when service_category_1 = 'pharmacy' then paid_amount else 0 end), 0)
            as pharmacy_paid_amount
        , coalesce(sum(case when service_category_1 = 'pharmacy' then allowed_amount else 0 end), 0)
            as pharmacy_allowed_amount
        , coalesce(sum(case when service_category_1 = 'other' then paid_amount else 0 end), 0)
            as other_paid_amount
        , coalesce(sum(case when service_category_1 = 'other' then allowed_amount else 0 end), 0)
            as other_allowed_amount
        , coalesce(sum(case
            when service_category_1 is null
              or service_category_1 not in ('inpatient', 'outpatient', 'office-based', 'ancillary', 'pharmacy', 'other')
              then paid_amount else 0
          end), 0) as unclassified_paid_amount
        , coalesce(sum(case
            when service_category_1 is null
              or service_category_1 not in ('inpatient', 'outpatient', 'office-based', 'ancillary', 'pharmacy', 'other')
              then allowed_amount else 0
          end), 0) as unclassified_allowed_amount
    from medical_claims
)

, pharmacy_claims as (
    select
        claim_id
        , data_source
        , enrollment_flag
        , paid_amount
        , allowed_amount
        , charge_amount
    from {{ ref('core__pharmacy_claim') }}
)

, distinct_pharmacy_claims as (
    select distinct
        claim_id
        , data_source
    from pharmacy_claims
    where claim_id is not null
)

, pharmacy_claim_metrics as (
    select
        count(*) as claim_line_count
        , (select count(*) from distinct_pharmacy_claims) as claim_count
        , coalesce(sum(case when coalesce(enrollment_flag, 0) = 1 then 1 else 0 end), 0)
            as enrolled_claim_line_count
        , coalesce(sum(case when coalesce(enrollment_flag, 0) <> 1 then 1 else 0 end), 0)
            as unenrolled_claim_line_count
        , coalesce(sum(paid_amount), 0) as paid_amount
        , coalesce(sum(allowed_amount), 0) as allowed_amount
        , coalesce(sum(charge_amount), 0) as charge_amount
    from pharmacy_claims
)

, combined_claim_metrics as (
    select
        medical_claim_metrics.paid_amount + pharmacy_claim_metrics.paid_amount as paid_amount
        , medical_claim_metrics.allowed_amount + pharmacy_claim_metrics.allowed_amount as allowed_amount
    from medical_claim_metrics
    cross join pharmacy_claim_metrics
)

, claim_encounters as (
    select
        person_id
        , data_source
        , lower(trim(cast(encounter_group as {{ dbt.type_string() }}))) as encounter_group
        , lower(trim(cast(encounter_type as {{ dbt.type_string() }}))) as encounter_type
        , paid_amount
        , allowed_amount
        , charge_amount
        , claim_count
    from {{ ref('core__encounter') }}
    where lower(trim(cast(encounter_source_type as {{ dbt.type_string() }}))) = 'claim'
)

, claim_encounter_people as (
    select distinct
        person_id
        , data_source
    from claim_encounters
    where person_id is not null
)

, encounter_metrics as (
    select
        count(*) as encounter_count
        , (select count(*) from claim_encounter_people) as encounter_person_count
        , coalesce(sum(paid_amount), 0) as paid_amount
        , coalesce(sum(allowed_amount), 0) as allowed_amount
        , coalesce(sum(charge_amount), 0) as charge_amount
        , coalesce(sum(claim_count), 0) as linked_claim_count
        , coalesce(sum(case when encounter_group = 'inpatient' then 1 else 0 end), 0) as inpatient_encounter_count
        , coalesce(sum(case when encounter_group = 'outpatient' then 1 else 0 end), 0) as outpatient_encounter_count
        , coalesce(sum(case when encounter_group = 'office based' then 1 else 0 end), 0) as office_based_encounter_count
        , coalesce(sum(case when encounter_group = 'other' then 1 else 0 end), 0) as other_encounter_count
        , coalesce(sum(case
            when encounter_group is null or encounter_group not in ('inpatient', 'outpatient', 'office based', 'other') then 1
            else 0
          end), 0) as unclassified_encounter_group_count
        , coalesce(sum(case when encounter_type = 'acute inpatient' then 1 else 0 end), 0) as acute_inpatient_count
        , coalesce(sum(case when encounter_type = 'ambulance - orphaned' then 1 else 0 end), 0) as ambulance_orphaned_count
        , coalesce(sum(case when encounter_type = 'ambulatory surgery center' then 1 else 0 end), 0) as ambulatory_surgery_center_count
        , coalesce(sum(case when encounter_type = 'dialysis' then 1 else 0 end), 0) as dialysis_count
        , coalesce(sum(case when encounter_type = 'dme - orphaned' then 1 else 0 end), 0) as dme_orphaned_count
        , coalesce(sum(case when encounter_type = 'emergency department' then 1 else 0 end), 0) as emergency_department_count
        , coalesce(sum(case when encounter_type = 'home health' then 1 else 0 end), 0) as home_health_count
        , coalesce(sum(case when encounter_type = 'inpatient hospice' then 1 else 0 end), 0) as inpatient_hospice_count
        , coalesce(sum(case when encounter_type = 'inpatient long term acute care' then 1 else 0 end), 0) as inpatient_long_term_acute_care_count
        , coalesce(sum(case when encounter_type = 'inpatient psych' then 1 else 0 end), 0) as inpatient_psych_count
        , coalesce(sum(case when encounter_type = 'inpatient rehabilitation' then 1 else 0 end), 0) as inpatient_rehabilitation_count
        , coalesce(sum(case when encounter_type = 'inpatient skilled nursing' then 1 else 0 end), 0) as inpatient_skilled_nursing_count
        , coalesce(sum(case when encounter_type = 'inpatient substance use' then 1 else 0 end), 0) as inpatient_substance_use_count
        , coalesce(sum(case when encounter_type = 'lab - orphaned' then 1 else 0 end), 0) as lab_orphaned_count
        , coalesce(sum(case when encounter_type = 'office visit' then 1 else 0 end), 0) as office_visit_count
        , coalesce(sum(case when encounter_type = 'office visit - other' then 1 else 0 end), 0) as office_visit_other_count
        , coalesce(sum(case when encounter_type = 'office visit injections' then 1 else 0 end), 0) as office_visit_injections_count
        , coalesce(sum(case when encounter_type = 'office visit pt/ot/st' then 1 else 0 end), 0) as office_visit_pt_ot_st_count
        , coalesce(sum(case when encounter_type = 'office visit radiology' then 1 else 0 end), 0) as office_visit_radiology_count
        , coalesce(sum(case when encounter_type = 'office visit surgery' then 1 else 0 end), 0) as office_visit_surgery_count
        , coalesce(sum(case when encounter_type = 'orphaned claim' then 1 else 0 end), 0) as orphaned_claim_count
        , coalesce(sum(case when encounter_type = 'outpatient hospice' then 1 else 0 end), 0) as outpatient_hospice_count
        , coalesce(sum(case when encounter_type = 'outpatient hospital or clinic' then 1 else 0 end), 0) as outpatient_hospital_or_clinic_count
        , coalesce(sum(case when encounter_type = 'outpatient injections' then 1 else 0 end), 0) as outpatient_injections_count
        , coalesce(sum(case when encounter_type = 'outpatient psych' then 1 else 0 end), 0) as outpatient_psych_count
        , coalesce(sum(case when encounter_type = 'outpatient pt/ot/st' then 1 else 0 end), 0) as outpatient_pt_ot_st_count
        , coalesce(sum(case when encounter_type = 'outpatient radiology' then 1 else 0 end), 0) as outpatient_radiology_count
        , coalesce(sum(case when encounter_type = 'outpatient rehabilitation' then 1 else 0 end), 0) as outpatient_rehabilitation_count
        , coalesce(sum(case when encounter_type = 'outpatient substance use' then 1 else 0 end), 0) as outpatient_substance_use_count
        , coalesce(sum(case when encounter_type = 'outpatient surgery' then 1 else 0 end), 0) as outpatient_surgery_count
        , coalesce(sum(case when encounter_type = 'telehealth' then 1 else 0 end), 0) as telehealth_count
        , coalesce(sum(case when encounter_type = 'urgent care' then 1 else 0 end), 0) as urgent_care_count
        , coalesce(sum(case
            when encounter_type is null or encounter_type not in (
                'acute inpatient'
                , 'ambulance - orphaned'
                , 'ambulatory surgery center'
                , 'dialysis'
                , 'dme - orphaned'
                , 'emergency department'
                , 'home health'
                , 'inpatient hospice'
                , 'inpatient long term acute care'
                , 'inpatient psych'
                , 'inpatient rehabilitation'
                , 'inpatient skilled nursing'
                , 'inpatient substance use'
                , 'lab - orphaned'
                , 'office visit'
                , 'office visit - other'
                , 'office visit injections'
                , 'office visit pt/ot/st'
                , 'office visit radiology'
                , 'office visit surgery'
                , 'orphaned claim'
                , 'outpatient hospice'
                , 'outpatient hospital or clinic'
                , 'outpatient injections'
                , 'outpatient psych'
                , 'outpatient pt/ot/st'
                , 'outpatient radiology'
                , 'outpatient rehabilitation'
                , 'outpatient substance use'
                , 'outpatient surgery'
                , 'telehealth'
                , 'urgent care'
            ) then 1
            else 0
          end), 0) as unclassified_encounter_type_count
    from claim_encounters
)

, condition_metrics as (
    select count(*) as condition_count
    from {{ ref('core__condition') }}
    where claim_id is not null
)

, procedure_metrics as (
    select count(*) as procedure_count
    from {{ ref('core__procedure') }}
    where claim_id is not null
)

, claims_preprocessing_metrics as (
    select
        (select count(*) from {{ ref('member_month__member_month') }}) as member_month_count
        , (select count(*) from {{ ref('service_category__service_category_grouper') }})
            as service_category_candidate_count
        , (select count(*)
           from {{ ref('service_category__service_category_grouper') }}
           where duplicate_row_number = 1) as primary_service_category_count
        , (select count(*) from {{ ref('claims_enrollment__flag_claims_with_enrollment') }})
            as medical_enrollment_match_count
        , (select count(*) from {{ ref('claims_enrollment__flag_rx_claims_with_enrollment') }})
            as pharmacy_enrollment_match_count
)

{% for metric_id, metric_name, source_cte, result_column in metric_catalog %}
select
    cast('{{ metric_id }}' as {{ dbt.type_string() }}) as metric_id
    , cast('{{ metric_name }}' as {{ dbt.type_string() }}) as metric_name
    , cast({{ result_column }} as {{ dbt.type_numeric() }}) as result
from {{ source_cte }}
{% if not loop.last %}
union all
{% endif %}
{% endfor %}
