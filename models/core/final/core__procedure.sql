{{ config(
     enabled = var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))
 | as_bool
   )
}}


with all_procedures as (
{% if var('clinical_enabled', var('tuva_marts_enabled',False)) == true and var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

select * from {{ ref('core__stg_claims_procedure') }}
union all
select * from {{ ref('core__stg_clinical_procedure') }}

{% elif var('clinical_enabled', var('tuva_marts_enabled',False)) == true -%}

select * from {{ ref('core__stg_clinical_procedure') }}

{% elif var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

select * from {{ ref('core__stg_claims_procedure') }}

{%- endif %}
)

{% if var('enable_normalize_engine',false) != true %}

select
    all_procedures.PROCEDURE_ID
  , all_procedures.PATIENT_ID
  , all_procedures.ENCOUNTER_ID
  , all_procedures.CLAIM_ID
  , all_procedures.PROCEDURE_DATE
  , all_procedures.SOURCE_CODE_TYPE
  , all_procedures.SOURCE_CODE
  , all_procedures.SOURCE_DESCRIPTION
  , case when all_procedures.NORMALIZED_CODE_TYPE is not null then  all_procedures.NORMALIZED_CODE_TYPE
      when icd10.icd_10_pcs is not null then 'icd-10-pcs'
      when icd9.icd_9_pcs is not null then 'icd-9-pcs'
      when hcpcs.hcpcs is not null then 'hcpcs'
      when snomed_ct.snomed_ct is not null then 'snomed-ct'
      end as NORMALIZED_CODE_TYPE
  , coalesce(all_procedures.NORMALIZED_CODE
      , icd10.icd_10_pcs
      , icd9.icd_9_pcs
      , hcpcs.hcpcs
      ,snomed_ct.snomed_ct ) as NORMALIZED_CODE
  ,  coalesce(all_procedures.NORMALIZED_DESCRIPTION
      , icd10.description
      , icd9.short_description
      , hcpcs.short_description
      , snomed_ct.description) NORMALIZED_DESCRIPTION
  , case when coalesce(all_procedures.NORMALIZED_CODE, all_procedures.NORMALIZED_DESCRIPTION) is not null then 'manual'
         when coalesce(icd10.icd_10_pcs,icd9.icd_9_pcs, hcpcs.hcpcs, snomed_ct.snomed_ct) is not null then 'automatic'
         end as mapping_method
  , all_procedures.MODIFIER_1
  , all_procedures.MODIFIER_2
  , all_procedures.MODIFIER_3
  , all_procedures.MODIFIER_4
  , all_procedures.MODIFIER_5
  , all_procedures.PRACTITIONER_ID
  , all_procedures.DATA_SOURCE
  , all_procedures.TUVA_LAST_RUN
from all_procedures
left join {{ ref('terminology__icd_10_pcs') }} icd10
    on all_procedures.source_code_type = 'icd-10-pcs'
        and all_procedures.source_code = icd10.icd_10_pcs
left join {{ ref('terminology__icd_9_pcs') }} icd9
    on all_procedures.source_code_type = 'icd-9-pcs'
        and all_procedures.source_code = icd9.icd_9_pcs
left join {{ ref('terminology__hcpcs_level_2') }} hcpcs
    on all_procedures.source_code_type = 'hcpcs'
        and all_procedures.source_code = hcpcs.hcpcs
left join {{ ref('terminology__snomed_ct')}} snomed_ct
    on all_procedures.source_code_type = 'snomed-ct'
        and all_procedures.source_code = snomed_ct.snomed_ct


{% else %}


select
    all_procedures.PROCEDURE_ID
  , all_procedures.PATIENT_ID
  , all_procedures.ENCOUNTER_ID
  , all_procedures.CLAIM_ID
  , all_procedures.PROCEDURE_DATE
  , all_procedures.SOURCE_CODE_TYPE
  , all_procedures.SOURCE_CODE
  , all_procedures.SOURCE_DESCRIPTION
  , case when all_procedures.NORMALIZED_CODE_TYPE is not null then  all_procedures.NORMALIZED_CODE_TYPE
      when icd10.icd_10_pcs is not null then 'icd-10-pcs'
      when icd9.icd_9_pcs is not null then 'icd-9-pcs'
      when hcpcs.hcpcs is not null then 'hcpcs'
      when snomed_ct.snomed_ct is not null then 'snomed-ct'
      else custom_mapped.normalized_code_type end as NORMALIZED_CODE_TYPE
  , coalesce(all_procedures.NORMALIZED_CODE
      , icd10.icd_10_pcs
      , icd9.icd_9_pcs
      , hcpcs.hcpcs
      ,snomed_ct.snomed_ct
      ,custom_mapped.normalized_code  ) as NORMALIZED_CODE
  ,  coalesce(all_procedures.NORMALIZED_DESCRIPTION
      , icd10.description
      , icd9.short_description
      , hcpcs.short_description
      , snomed_ct.description
      , custom_mapped.normalized_description) as NORMALIZED_DESCRIPTION
  , case when coalesce(all_procedures.NORMALIZED_CODE ,all_procedures.NORMALIZED_DESCRIPTION) is not null then 'manual'
         when coalesce(icd10.icd_10_pcs,icd9.icd_9_pcs, hcpcs.hcpcs, snomed_ct.snomed_ct) is not null then 'automatic'
         when custom_mapped.not_mapped is not null then custom_mapped.not_mapped
         when coalesce(custom_mapped.normalized_code,custom_mapped.normalized_description) is not null then 'custom'
         end as mapping_method
  , all_procedures.MODIFIER_1
  , all_procedures.MODIFIER_2
  , all_procedures.MODIFIER_3
  , all_procedures.MODIFIER_4
  , all_procedures.MODIFIER_5
  , all_procedures.PRACTITIONER_ID
  , all_procedures.DATA_SOURCE
  , all_procedures.TUVA_LAST_RUN
from all_procedures
left join {{ ref('terminology__icd_10_pcs') }} icd10
    on all_procedures.source_code_type = 'icd-10-pcs'
        and all_procedures.source_code = icd10.icd_10_pcs
left join {{ ref('terminology__icd_9_pcs') }} icd9
    on all_procedures.source_code_type = 'icd-9-pcs'
        and all_procedures.source_code = icd9.icd_9_pcs
left join {{ ref('terminology__hcpcs_level_2') }} hcpcs
    on all_procedures.source_code_type = 'hcpcs'
        and all_procedures.source_code = hcpcs.hcpcs
left join {{ ref('terminology__snomed_ct')}} snomed_ct
    on all_procedures.source_code_type = 'snomed-ct'
        and all_procedures.source_code = snomed_ct.snomed_ct
left join {{ ref('custom_mapped') }} custom_mapped
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