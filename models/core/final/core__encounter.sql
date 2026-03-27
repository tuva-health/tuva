{{ config(
     enabled = (var('claims_enabled', var('tuva_marts_enabled', False)) | as_bool)
            or (var('clinical_enabled', var('tuva_marts_enabled', False)) | as_bool)
   )
}}

{%- set tuva_core_columns -%}
      encounter_id
    , person_id
    , encounter_type
    , encounter_group
    , encounter_start_date
    , encounter_end_date
    , length_of_stay
    , admit_source_code
    , admit_source_description
    , admit_type_code
    , admit_type_description
    , discharge_disposition_code
    , discharge_disposition_description
    , attending_provider_id
    , attending_provider_name
    , facility_id
    , facility_name
    , facility_type
    , observation_flag
    , lab_flag
    , dme_flag
    , ambulance_flag
    , pharmacy_flag
    , ed_flag
    , delivery_flag
    , delivery_type
    , newborn_flag
    , nicu_flag
    , snf_part_b_flag
    , primary_diagnosis_code_type
    , primary_diagnosis_code
    , primary_diagnosis_description
    , drg_code_type
    , drg_code
    , drg_description
    , paid_amount
    , allowed_amount
    , charge_amount
    , claim_count
    , inst_claim_count
    , prof_claim_count
    , source_model
    , encounter_source_type
{%- endset -%}

{%- set tuva_metadata_columns -%}
    , data_source
    , tuva_last_run
{%- endset -%}

{% if var('clinical_enabled', false) == true and var('claims_enabled', false) == true -%}

{%- set tuva_extension_columns -%}
    {{ select_extension_columns(ref('input_layer__encounter')) }}
{%- endset -%}

with enc as (
    {{ smart_union([ref('core__stg_claims_encounter'), ref('core__stg_clinical_encounter')], source_index=none) }}
)

select
    {{ tuva_core_columns }}
    {{ tuva_extension_columns }}
    {{ tuva_metadata_columns }}
from enc

{% elif var('clinical_enabled', var('tuva_marts_enabled',False)) == true -%}

{%- set tuva_extension_columns -%}
    {{ select_extension_columns(ref('input_layer__encounter')) }}
{%- endset -%}

select
    {{ tuva_core_columns }}
    {{ tuva_extension_columns }}
    {{ tuva_metadata_columns }}
from {{ ref('core__stg_clinical_encounter') }}

{% elif var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

{%- set tuva_extension_columns -%}
{# No extension columns — input_layer__encounter is clinical-only #}
{%- endset -%}

select
    {{ tuva_core_columns }}
    {{ tuva_extension_columns }}
    {{ tuva_metadata_columns }}
from {{ ref('core__stg_claims_encounter') }}

{%- endif %}
