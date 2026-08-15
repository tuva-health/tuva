
{{ config(
     enabled = (var('claims_enabled', False) | string | lower) == 'true'
   )
}}

-- *************************************************
-- This dbt model creates the encounter table in core using dbt_utils.union_relations.
-- *************************************************

with base as (
  {{ dbt_utils.union_relations(
    relations=[
       ref('encounter__acute_inpatient')
      , ref('encounter__emergency_department')
      , ref('encounter__inpatient_hospice')
      , ref('encounter__inpatient_psychiatric')
      , ref('encounter__inpatient_rehab')
      , ref('encounter__inpatient_skilled_nursing')
      , ref('encounter__inpatient_substance_use')
      , ref('encounter__inpatient_long_term')
      , ref('encounter__urgent_care')
      , ref('encounter__office_visit')
      , ref('encounter__outpatient_hospice')
      , ref('encounter__outpatient_hospital_or_clinic')
      , ref('encounter__outpatient_injections')
      , ref('encounter__outpatient_psychiatric')
      , ref('encounter__outpatient_therapy')
      , ref('encounter__outpatient_surgery')
      , ref('encounter__outpatient_radiology')
      , ref('encounter__outpatient_rehab')
      , ref('encounter__outpatient_substance_use')
      , ref('encounter__home_health')
      , ref('encounter__dialysis')
      , ref('encounter__ambulatory_surgery_center')
      , ref('encounter__ambulance')
      , ref('encounter__dme')
      , ref('encounter__lab')
      , ref('encounter__orphaned_claim')
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
  , cast(facility_npi as {{ dbt.type_string() }}) as facility_npi
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
  , cast('claim' as {{ dbt.type_string() }}) as encounter_source_type
  , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
  , cast(base.data_source as {{ dbt.type_string() }}) as data_source
from base
inner join {{ ref('normalized__patient_data_source_id') }} as p on base.patient_data_source_id = p.patient_data_source_id
