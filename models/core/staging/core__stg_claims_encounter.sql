
{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False)) | as_bool
   )
}}

-- *************************************************
-- This dbt model creates the encounter table in core using dbt_utils.union_relations.
-- *************************************************

{{ config(materialized='table') }}

with base as (
  {{ dbt_utils.union_relations(
    relations=[
       ref('acute_inpatient__encounter_grain')
      , ref('emergency_department__encounter_grain')
      , ref('inpatient_hospice__encounter_grain')
      , ref('inpatient_psych__encounter_grain')
      , ref('inpatient_rehab__encounter_grain')
      , ref('inpatient_snf__encounter_grain')
      , ref('inpatient_substance_use__encounter_grain')
      , ref('inpatient_long_term__encounter_grain')
      , ref('urgent_care__encounter_grain')
      , ref('office_visit__encounter_grain')
      , ref('outpatient_hospice__encounter_grain')
      , ref('outpatient_hospital_or_clinic__encounter_grain')
      , ref('outpatient_injections__encounter_grain')
      , ref('outpatient_psych__encounter_grain')
      , ref('outpatient_ptotst__encounter_grain')
      , ref('outpatient_surgery__encounter_grain')
      , ref('outpatient_radiology__encounter_grain')
      , ref('outpatient_rehab__encounter_grain')
      , ref('outpatient_substance_use__encounter_grain')
      , ref('home_health__encounter_grain')
      , ref('dialysis__encounter_grain')
      , ref('asc__encounter_grain')
      , ref('ambulance__encounter_grain')
      , ref('dme__encounter_grain')
      , ref('lab__encounter_grain')
      , ref('orphaned_claim__encounter_grain')
      ],
    exclude=["_loaded_at"]
  ) }}
)

select
    cast(encounter_id as {{ dbt.type_string() }}) as encounter_id
  , cast(p.person_id as {{ dbt.type_string() }}) as person_id
  , cast(encounter_type as {{ dbt.type_string() }}) as encounter_type
  , cast(encounter_group as {{ dbt.type_string() }}) as encounter_group
  , {{ try_to_cast_date('encounter_start_date', 'YYYY-MM-DD') }} as encounter_start_date
  , coalesce({{ try_to_cast_date('encounter_end_date', 'YYYY-MM-DD') }}, {{ try_to_cast_date('encounter_start_date', 'YYYY-MM-DD') }}) as encounter_end_date
  , cast(length_of_stay as {{ dbt.type_int() }}) as length_of_stay
  , cast(admit_source_code as {{ dbt.type_string() }}) as admit_source_code
  , cast(admit_source_description as {{ dbt.type_string() }}) as admit_source_description
  , cast(admit_type_code as {{ dbt.type_string() }}) as admit_type_code
  , cast(admit_type_description as {{ dbt.type_string() }}) as admit_type_description
  , cast(discharge_disposition_code as {{ dbt.type_string() }}) as discharge_disposition_code
  , cast(discharge_disposition_description as {{ dbt.type_string() }}) as discharge_disposition_description
  , cast(null as {{ dbt.type_string() }}) as attending_provider_id
  , cast(null as {{ dbt.type_string() }}) as attending_provider_name
  , cast(facility_id as {{ dbt.type_string() }}) as facility_id
  , cast(facility_name as {{ dbt.type_string() }}) as facility_name
  , cast(facility_type as {{ dbt.type_string() }}) as facility_type
  , cast(coalesce(observation_flag, 0) as {{ dbt.type_int() }}) as observation_flag
  , cast(coalesce(lab_flag, 0) as {{ dbt.type_int() }}) as lab_flag
  , cast(coalesce(dme_flag, 0) as {{ dbt.type_int() }}) as dme_flag
  , cast(coalesce(ambulance_flag, 0) as {{ dbt.type_int() }}) as ambulance_flag
  , cast(coalesce(pharmacy_flag, 0) as {{ dbt.type_int() }}) as pharmacy_flag
  , cast(coalesce(ed_flag, 0) as {{ dbt.type_int() }}) as ed_flag
  , cast(coalesce(delivery_flag, 0) as {{ dbt.type_int() }}) as delivery_flag
  , cast(delivery_type as {{ dbt.type_string() }}) as delivery_type
  , cast(coalesce(newborn_flag, 0) as {{ dbt.type_int() }}) as newborn_flag
  , cast(coalesce(nicu_flag, 0) as {{ dbt.type_int() }}) as nicu_flag
  , cast(coalesce(snf_part_b_flag, 0) as {{ dbt.type_int() }}) as snf_part_b_flag
  , cast(primary_diagnosis_code_type as {{ dbt.type_string() }}) as primary_diagnosis_code_type
  , cast(primary_diagnosis_code as {{ dbt.type_string() }}) as primary_diagnosis_code
  , cast(primary_diagnosis_description as {{ dbt.type_string() }}) as primary_diagnosis_description
  , cast(drg_code_type as {{ dbt.type_string() }}) as drg_code_type
  , cast(drg_code as {{ dbt.type_string() }}) as drg_code
  , cast(drg_description as {{ dbt.type_string() }}) as drg_description
  , cast(total_paid_amount as {{ dbt.type_numeric() }}) as paid_amount
  , cast(total_allowed_amount as {{ dbt.type_numeric() }}) as allowed_amount
  , cast(total_charge_amount as {{ dbt.type_numeric() }}) as charge_amount
  , cast(claim_count as {{ dbt.type_int() }}) as claim_count
  , cast(inst_claim_count as {{ dbt.type_int() }}) as inst_claim_count
  , cast(prof_claim_count as {{ dbt.type_int() }}) as prof_claim_count
  , cast(_dbt_source_relation as {{ dbt.type_string() }}) as source_model
  , cast(base.data_source as {{ dbt.type_string() }}) as data_source
  , cast('claim' as {{ dbt.type_string() }}) as encounter_source_type
  , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from base
inner join {{ ref('encounters__patient_data_source_id') }} as p on base.patient_data_source_id = p.patient_data_source_id
