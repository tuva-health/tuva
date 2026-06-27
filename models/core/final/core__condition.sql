{{ config(
     enabled = (var('claims_enabled', False) | as_bool)
            or (var('clinical_enabled', False) | as_bool)
   )
}}

{%- set tuva_extension_columns_from_all_conditions -%}
{% if var('clinical_enabled', False) | as_bool %}
    {{ select_extension_columns(ref('normalized__condition'), alias='all_conditions') }}
{% endif %}
{%- endset -%}

{%- set tuva_metadata_columns_from_all_conditions -%}
    , all_conditions.tuva_last_run
    , all_conditions.data_source
{%- endset -%}

with all_conditions as (
{% if var('clinical_enabled', False) == true
    and var('claims_enabled', False) == true -%}

    {{ smart_union([ref('int_condition_from_claims'), ref('normalized__condition')]) }}

{% elif var('clinical_enabled', False) == true -%}

    select *
    from {{ ref('normalized__condition') }}

{% elif var('claims_enabled', False) == true -%}

    select *
    from {{ ref('int_condition_from_claims') }}

{%- endif %}
)

select
    all_conditions.condition_id
  , all_conditions.person_id
  , all_conditions.member_id
  , all_conditions.patient_id
  , all_conditions.encounter_id
  , all_conditions.claim_id
  , all_conditions.payer
  , all_conditions.recorded_date
  , all_conditions.onset_date
  , all_conditions.resolved_date
  , all_conditions.status
  , all_conditions.condition_type
  , all_conditions.code_system
  , all_conditions.source_code
  , all_conditions.source_description
  , all_conditions.normalized_code
  , all_conditions.normalized_description
  , condition_grouper.condition_family
  , condition_grouper.condition
  , all_conditions.condition_rank
  , all_conditions.present_on_admit_code
  , all_conditions.present_on_admit_description
  {{ tuva_extension_columns_from_all_conditions }}
  {{ tuva_metadata_columns_from_all_conditions }}
from all_conditions
left join {{ ref('tuva_condition_grouper_code_map') }} as condition_grouper
    on lower(all_conditions.code_system) = condition_grouper.code_system
        and replace(all_conditions.normalized_code, '.', '') = condition_grouper.code
