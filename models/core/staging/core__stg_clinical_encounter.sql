{{ config(
     enabled = var('clinical_enabled',var('tuva_marts_enabled',False)) | as_bool
   )
}}

{%- set tuva_core_columns -%}
      cast(encounter_id as {{ dbt.type_string() }}) as encounter_id
    , cast(person_id as {{ dbt.type_string() }}) as person_id
    , cast(encounter_type as {{ dbt.type_string() }}) as encounter_type
    , cast('clinical' as {{ dbt.type_string() }}) as encounter_group
    , {{ try_to_cast_date('encounter_start_date', 'YYYY-MM-DD') }} as encounter_start_date
    , {{ try_to_cast_date('encounter_end_date', 'YYYY-MM-DD') }} as encounter_end_date
    , cast(length_of_stay as {{ dbt.type_int() }}) as length_of_stay
    , cast(admit_source_code as {{ dbt.type_string() }}) as admit_source_code
    , cast(admit_source_description as {{ dbt.type_string() }}) as admit_source_description
    , cast(admit_type_code as {{ dbt.type_string() }}) as admit_type_code
    , cast(admit_type_description as {{ dbt.type_string() }}) as admit_type_description
    , cast(discharge_disposition_code as {{ dbt.type_string() }}) as discharge_disposition_code
    , cast(discharge_disposition_description as {{ dbt.type_string() }}) as discharge_disposition_description
    , cast(attending_provider_id as {{ dbt.type_string() }}) as attending_provider_id
    , cast(attending_provider_name as {{ dbt.type_string() }}) as attending_provider_name
    , cast(facility_id as {{ dbt.type_string() }}) as facility_id
    , cast(facility_name as {{ dbt.type_string() }}) as facility_name
    , cast(null as {{ dbt.type_string() }}) as facility_type
    , cast(null as {{ dbt.type_int() }}) as observation_flag
    , cast(null as {{ dbt.type_int() }}) as lab_flag
    , cast(null as {{ dbt.type_int() }}) as dme_flag
    , cast(null as {{ dbt.type_int() }}) as ambulance_flag
    , cast(null as {{ dbt.type_int() }}) as pharmacy_flag
    , cast(null as {{ dbt.type_int() }}) as ed_flag
    , cast(null as {{ dbt.type_int() }}) as delivery_flag
    , cast(null as {{ dbt.type_string() }}) as delivery_type
    , cast(null as {{ dbt.type_int() }}) as newborn_flag
    , cast(null as {{ dbt.type_int() }}) as nicu_flag
    , cast(null as {{ dbt.type_int() }}) as snf_part_b_flag
    , cast(primary_diagnosis_code_type as {{ dbt.type_string() }}) as primary_diagnosis_code_type
    , cast(primary_diagnosis_code as {{ dbt.type_string() }}) as primary_diagnosis_code
    , cast(primary_diagnosis_description as {{ dbt.type_string() }}) as primary_diagnosis_description
    , cast(drg_code_type as {{ dbt.type_string() }}) as drg_code_type
    , cast(drg_code as {{ dbt.type_string() }}) as drg_code
    , cast(drg_description as {{ dbt.type_string() }}) as drg_description
    , cast(paid_amount as {{ dbt.type_numeric() }}) as paid_amount
    , cast(allowed_amount as {{ dbt.type_numeric() }}) as allowed_amount
    , cast(charge_amount as {{ dbt.type_numeric() }}) as charge_amount
    , cast(null as {{ dbt.type_int() }}) as claim_count
    , cast(null as {{ dbt.type_int() }}) as inst_claim_count
    , cast(null as {{ dbt.type_int() }}) as prof_claim_count
    , cast(null as {{ dbt.type_string() }}) as source_model
    , cast('clinical' as {{ dbt.type_string() }}) as encounter_source_type
{%- endset -%}

{%- set tuva_metadata_columns -%}
      , cast(data_source as {{ dbt.type_string() }}) as data_source
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
{%- endset %}

{%- set tuva_extension_columns -%}
    {{ select_extension_columns(ref('input_layer__encounter'), strip_prefix=false) }}
{%- endset %}

select
    {{ tuva_core_columns }}
    {{ tuva_extension_columns }}
    {{ tuva_metadata_columns }}
from {{ ref('input_layer__encounter') }}
