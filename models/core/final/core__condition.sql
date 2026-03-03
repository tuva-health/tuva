{{ config(
     enabled = (var('claims_enabled', var('tuva_marts_enabled', False)) | as_bool)
            or (var('clinical_enabled', var('tuva_marts_enabled', False)) | as_bool)
   )
}}

{%- set tuva_extension_columns -%}
{% if var('clinical_enabled', var('tuva_marts_enabled', False)) | as_bool %}
    {{ select_extension_columns(ref('input_layer__condition')) }}
{% endif %}
{%- endset -%}

{%- set tuva_metadata_columns -%}
    , all_conditions.data_source
    , all_conditions.tuva_last_run
{%- endset -%}

with all_conditions as (
{% if var('clinical_enabled', var('tuva_marts_enabled', False)) == true
    and var('claims_enabled', var('tuva_marts_enabled', False)) == true -%}

    {{ smart_union([ref('core__stg_claims_condition'), ref('core__stg_clinical_condition')]) }}

{% elif var('clinical_enabled', var('tuva_marts_enabled',False)) == true -%}

    select *
    from {{ ref('core__stg_clinical_condition') }}

{% elif var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

    select *
    from {{ ref('core__stg_claims_condition') }}

{%- endif %}
)


{# is the default code that gets executed as long as an enable_normalize_engine var is false or not defined #}
{% if var('enable_normalize_engine',false) != true %}
select
    all_conditions.condition_id
  , all_conditions.payer
  , all_conditions.person_id
  , all_conditions.member_id
  , all_conditions.patient_id
  , all_conditions.encounter_id
  , all_conditions.claim_id
  , all_conditions.recorded_date
  , all_conditions.onset_date
  , all_conditions.resolved_date
  , all_conditions.status
  , all_conditions.condition_type
  , all_conditions.source_code_type
  , all_conditions.source_code
  , all_conditions.source_description
  , case
        when all_conditions.normalized_code_type is not null then all_conditions.normalized_code_type
        when icd10.icd_10_cm is not null then 'icd-10-cm'
        when icd9.icd_9_cm is not null then 'icd-9-cm'
        when snomed_ct.snomed_ct is not null then 'snomed-ct'
        else null end as normalized_code_type
  , coalesce(
        all_conditions.normalized_code
      , icd10.icd_10_cm
      , icd9.icd_9_cm
      , snomed_ct.snomed_ct) as normalized_code
  , coalesce(
        all_conditions.normalized_description
      , icd10.short_description
      , icd9.short_description
      , snomed_ct.description) as normalized_description
  , case when coalesce(all_conditions.normalized_code, all_conditions.normalized_description) is not null then 'manual'
         when coalesce(icd10.icd_10_cm, icd9.icd_9_cm, snomed_ct.snomed_ct) is not null then 'automatic'
         end as mapping_method
  , all_conditions.condition_rank
  , all_conditions.present_on_admit_code
  , all_conditions.present_on_admit_description
   {{ tuva_extension_columns }}
   {{ tuva_metadata_columns }}
from
all_conditions
left join {{ ref('terminology__icd_10_cm') }} as icd10
    on all_conditions.source_code_type = 'icd-10-cm'
        and replace(all_conditions.source_code, '.', '') = icd10.icd_10_cm
left join {{ ref('terminology__icd_9_cm') }} as icd9
    on all_conditions.source_code_type = 'icd-9-cm'
        and replace(all_conditions.source_code, '.', '') = icd9.icd_9_cm
left join {{ ref('terminology__snomed_ct') }} as snomed_ct
    on all_conditions.source_code_type = 'snomed-ct'
        and all_conditions.source_code = snomed_ct.snomed_ct



{#  This code is only exectued if an enable_normalize_engine var is defined and set to true
    it expects a seed file called  #}
{% else %}
select
    all_conditions.condition_id
  , all_conditions.payer
  , all_conditions.person_id
  , all_conditions.member_id
  , all_conditions.patient_id
  , all_conditions.encounter_id
  , all_conditions.claim_id
  , all_conditions.recorded_date
  , all_conditions.onset_date
  , all_conditions.resolved_date
  , all_conditions.status
  , all_conditions.condition_type
  , all_conditions.source_code_type
  , all_conditions.source_code
  , all_conditions.source_description
  , case
        when all_conditions.normalized_code_type is not null then all_conditions.normalized_code_type
        when icd10.icd_10_cm is not null then 'icd-10-cm'
        when icd9.icd_9_cm is not null then 'icd-9-cm'
        when snomed_ct.snomed_ct is not null then 'snomed-ct'
        else custom_mapped.normalized_code_type end as normalized_code_type
  , coalesce(
        all_conditions.normalized_code
      , icd10.icd_10_cm
      , icd9.icd_9_cm
      , snomed_ct.snomed_ct
      , custom_mapped.normalized_code
      ) as normalized_code
  , coalesce(
        all_conditions.normalized_description
      , icd10.short_description
      , icd9.short_description
      , snomed_ct.description
      , custom_mapped.normalized_description
      ) as normalized_description
  , case when coalesce(all_conditions.normalized_code, all_conditions.normalized_description) is not null then 'manual'
         when coalesce(icd10.icd_10_cm,icd9.icd_9_cm, snomed_ct.snomed_ct) is not null then 'automatic'
         when custom_mapped.not_mapped is not null then custom_mapped.not_mapped
         when coalesce(custom_mapped.normalized_code,custom_mapped.normalized_description) is not null then 'custom'
         end as mapping_method
  , all_conditions.condition_rank
  , all_conditions.present_on_admit_code
  , all_conditions.present_on_admit_description
    {{ tuva_extension_columns }}
    {{ tuva_metadata_columns }}
from
all_conditions
left join {{ ref('terminology__icd_10_cm') }} as icd10
    on all_conditions.source_code_type = 'icd-10-cm'
        and replace(all_conditions.source_code,'.','') = icd10.icd_10_cm
left join {{ ref('terminology__icd_9_cm') }} as icd9
    on all_conditions.source_code_type = 'icd-9-cm'
        and replace(all_conditions.source_code,'.','') = icd9.icd_9_cm
left join {{ ref('terminology__snomed_ct') }} as snomed_ct
    on all_conditions.source_code_type = 'snomed-ct'
        and all_conditions.source_code = snomed_ct.snomed_ct
left join {{ ref('custom_mapped') }} as custom_mapped
    on  ( lower(all_conditions.source_code_type) = lower(custom_mapped.source_code_type)
        or ( all_conditions.source_code_type is null and custom_mapped.source_code_type is null)
        )
    and (all_conditions.source_code = custom_mapped.source_code
        or ( all_conditions.source_code is null and custom_mapped.source_code is null)
        )
    and (all_conditions.source_description = custom_mapped.source_description
        or ( all_conditions.source_description is null and custom_mapped.source_description is null)
        )
    and not (all_conditions.source_code is null and all_conditions.source_description is null)
{% endif %}
