{{ config(
     enabled = (var('claims_enabled', var('tuva_marts_enabled', False)) | as_bool)
            or (var('clinical_enabled', var('tuva_marts_enabled', False)) | as_bool)
   )
}}

{%- set tuva_extension_columns -%}
{% if var('clinical_enabled', var('tuva_marts_enabled', False)) | as_bool %}
    {{ select_extension_columns(ref('input_layer__procedure')) }}
{% endif %}
{%- endset -%}

{%- set tuva_metadata_columns -%}
    , all_procedures.data_source
    , all_procedures.tuva_last_run
{%- endset -%}

with all_procedures as (
{% if var('clinical_enabled', var('tuva_marts_enabled',False)) == true and var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

{{ smart_union([ref('core__stg_claims_procedure'), ref('core__stg_clinical_procedure')]) }}

{% elif var('clinical_enabled', var('tuva_marts_enabled',False)) == true -%}

select * from {{ ref('core__stg_clinical_procedure') }}

{% elif var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

select * from {{ ref('core__stg_claims_procedure') }}

{%- endif %}
)

{% if var('enable_normalize_engine',false) != true %}

select
    all_procedures.procedure_id
  , all_procedures.person_id
  , all_procedures.member_id
  , all_procedures.patient_id
  , all_procedures.encounter_id
  , all_procedures.claim_id
  , all_procedures.procedure_date
  , all_procedures.source_code_type
  , all_procedures.source_code
  , all_procedures.source_description
  , case when all_procedures.normalized_code_type is not null then all_procedures.normalized_code_type
      when icd10.icd_10_pcs is not null then 'icd-10-pcs'
      when icd9.icd_9_pcs is not null then 'icd-9-pcs'
      when hcpcs.hcpcs is not null then 'hcpcs'
      when snomed_ct.snomed_ct is not null then 'snomed-ct'
      end as normalized_code_type
  , coalesce(all_procedures.normalized_code
      , icd10.icd_10_pcs
      , icd9.icd_9_pcs
      , hcpcs.hcpcs
      , snomed_ct.snomed_ct) as normalized_code
  , coalesce(all_procedures.normalized_description
      , icd10.description
      , icd9.short_description
      , hcpcs.short_description
      , snomed_ct.description) as normalized_description
  , case when coalesce(all_procedures.normalized_code, all_procedures.normalized_description) is not null then 'manual'
         when coalesce(icd10.icd_10_pcs, icd9.icd_9_pcs, hcpcs.hcpcs, snomed_ct.snomed_ct) is not null then 'automatic'
         end as mapping_method
  , all_procedures.modifier_1
  , all_procedures.modifier_2
  , all_procedures.modifier_3
  , all_procedures.modifier_4
  , all_procedures.modifier_5
  , all_procedures.practitioner_id
  {{ tuva_extension_columns }}
  {{ tuva_metadata_columns }}
from all_procedures
left join {{ ref('terminology__icd_10_pcs') }} as icd10
    on all_procedures.source_code_type = 'icd-10-pcs'
        and all_procedures.source_code = icd10.icd_10_pcs
left join {{ ref('terminology__icd_9_pcs') }} as icd9
    on all_procedures.source_code_type = 'icd-9-pcs'
        and all_procedures.source_code = icd9.icd_9_pcs
left join {{ ref('terminology__hcpcs_level_2') }} as hcpcs
    on all_procedures.source_code_type = 'hcpcs'
        and all_procedures.source_code = hcpcs.hcpcs
left join {{ ref('terminology__snomed_ct') }} as snomed_ct
    on all_procedures.source_code_type = 'snomed-ct'
        and all_procedures.source_code = snomed_ct.snomed_ct


{% else %}


select
    all_procedures.procedure_id
  , all_procedures.person_id
  , all_procedures.member_id
  , all_procedures.patient_id
  , all_procedures.encounter_id
  , all_procedures.claim_id
  , all_procedures.procedure_date
  , all_procedures.source_code_type
  , all_procedures.source_code
  , all_procedures.source_description
  , case when all_procedures.normalized_code_type is not null then  all_procedures.normalized_code_type
      when icd10.icd_10_pcs is not null then 'icd-10-pcs'
      when icd9.icd_9_pcs is not null then 'icd-9-pcs'
      when hcpcs.hcpcs is not null then 'hcpcs'
      when snomed_ct.snomed_ct is not null then 'snomed-ct'
      else custom_mapped.normalized_code_type end as normalized_code_type
  , coalesce(all_procedures.normalized_code
      , icd10.icd_10_pcs
      , icd9.icd_9_pcs
      , hcpcs.hcpcs
      ,snomed_ct.snomed_ct
      ,custom_mapped.normalized_code  ) as normalized_code
  ,  coalesce(all_procedures.normalized_description
      , icd10.description
      , icd9.short_description
      , hcpcs.short_description
      , snomed_ct.description
      , custom_mapped.normalized_description) as normalized_description
  , case when coalesce(all_procedures.normalized_code ,all_procedures.normalized_description) is not null then 'manual'
         when coalesce(icd10.icd_10_pcs,icd9.icd_9_pcs, hcpcs.hcpcs, snomed_ct.snomed_ct) is not null then 'automatic'
         when custom_mapped.not_mapped is not null then custom_mapped.not_mapped
         when coalesce(custom_mapped.normalized_code,custom_mapped.normalized_description) is not null then 'custom'
         end as mapping_method
  , all_procedures.modifier_1
  , all_procedures.modifier_2
  , all_procedures.modifier_3
  , all_procedures.modifier_4
  , all_procedures.modifier_5
  , all_procedures.practitioner_id
  {{ tuva_extension_columns }}
  {{ tuva_metadata_columns }}
from all_procedures
left join {{ ref('terminology__icd_10_pcs') }} as icd10
    on all_procedures.source_code_type = 'icd-10-pcs'
        and all_procedures.source_code = icd10.icd_10_pcs
left join {{ ref('terminology__icd_9_pcs') }} as icd9
    on all_procedures.source_code_type = 'icd-9-pcs'
        and all_procedures.source_code = icd9.icd_9_pcs
left join {{ ref('terminology__hcpcs_level_2') }} as hcpcs
    on all_procedures.source_code_type = 'hcpcs'
        and all_procedures.source_code = hcpcs.hcpcs
left join {{ ref('terminology__snomed_ct') }} as snomed_ct
    on all_procedures.source_code_type = 'snomed-ct'
        and all_procedures.source_code = snomed_ct.snomed_ct
left join {{ ref('custom_mapped') }} as custom_mapped
    on ( lower(all_procedures.source_code_type) = lower(custom_mapped.source_code_type)
        or ( all_procedures.source_code_type is null and custom_mapped.source_code_type is null)
        )
        and (all_procedures.source_code = custom_mapped.source_code
            or ( all_procedures.source_code is null and custom_mapped.source_code is null)
            )
        and (all_procedures.source_description = custom_mapped.source_description
            or ( all_procedures.source_description is null and custom_mapped.source_description is null)
            )
        and not (all_procedures.source_code is null and all_procedures.source_description is null)

{% endif %}
