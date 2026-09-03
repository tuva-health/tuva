{{ config(
     enabled = (the_tuva_project.tuva_boolean_var('claims_enabled', false))
            or (the_tuva_project.tuva_boolean_var('clinical_enabled', false))
   )
}}

{%- set tuva_extension_columns_from_all_conditions -%}
{% if the_tuva_project.tuva_boolean_var('clinical_enabled', false) %}
    {{ select_extension_columns(ref('normalized__condition'), alias='all_conditions') }}
{% endif %}
{%- endset -%}

{%- set tuva_metadata_columns_from_all_conditions -%}
    , all_conditions.ingest_datetime
    , all_conditions.tuva_last_run
    , all_conditions.data_source
{%- endset -%}

with all_conditions as (
{% if the_tuva_project.tuva_boolean_var('clinical_enabled', false) == true
    and the_tuva_project.tuva_boolean_var('claims_enabled', false) == true -%}

    {{ smart_union([ref('int_condition_from_claims'), ref('normalized__condition')]) }}

{% elif the_tuva_project.tuva_boolean_var('clinical_enabled', false) == true -%}

    select *
    from {{ ref('normalized__condition') }}

{% elif the_tuva_project.tuva_boolean_var('claims_enabled', false) == true -%}

    select *
    from {{ ref('int_condition_from_claims') }}

{%- endif %}
)

, active_condition_grouper_candidates as (

    select
        lower({{ the_tuva_project.trim('code_system') }}) as code_system
      , case
            when lower({{ the_tuva_project.trim('code_system') }}) = 'icd-10-cm'
                then upper(replace({{ the_tuva_project.trim('code') }}, '.', ''))
            else {{ the_tuva_project.trim('code') }}
        end as code
      , condition_family
      , condition_name
    from {{ ref('tuva_condition_grouper_code_map') }}
    where lower({{ the_tuva_project.trim('status') }}) = 'active'
      and lower({{ the_tuva_project.trim('code_system') }}) in ('icd-10-cm', 'snomed-ct')

)

, condition_grouper as (

    select
        candidates.code_system
      , candidates.code
      , max(candidates.condition_family) as condition_family
      , max(candidates.condition_name) as condition_name
    from active_condition_grouper_candidates as candidates
    group by
        candidates.code_system
      , candidates.code
    -- Collapse identical duplicates, but fail closed when one normalized code
    -- has multiple active targets or an incomplete target.
    having count(candidates.condition_family) = count(*)
       and count(candidates.condition_name) = count(*)
       and min(candidates.condition_family) = max(candidates.condition_family)
       and min(candidates.condition_name) = max(candidates.condition_name)

)

select
    all_conditions.condition_id
  , all_conditions.source_condition_id
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
  , condition_grouper.condition_name
  , all_conditions.condition_rank
  , all_conditions.present_on_admit_code
  , all_conditions.present_on_admit_description
  {{ tuva_extension_columns_from_all_conditions }}
  {{ tuva_metadata_columns_from_all_conditions }}
from all_conditions
left join condition_grouper
    on lower({{ the_tuva_project.trim('all_conditions.code_system') }}) = condition_grouper.code_system
    and case
            when lower({{ the_tuva_project.trim('all_conditions.code_system') }}) = 'icd-10-cm'
                then upper(replace({{ the_tuva_project.trim('all_conditions.normalized_code') }}, '.', ''))
            else {{ the_tuva_project.trim('all_conditions.normalized_code') }}
        end = condition_grouper.code
