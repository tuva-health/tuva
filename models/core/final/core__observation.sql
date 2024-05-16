{{ config(
     enabled = var('clinical_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

{% if var('enable_normalize_engine',false) != true %}
select
      obs.OBSERVATION_ID
    , obs.PATIENT_ID
    , obs.ENCOUNTER_ID
    , obs.PANEL_ID
    , obs.OBSERVATION_DATE
    , obs.OBSERVATION_TYPE
    , obs.SOURCE_CODE_TYPE
    , obs.SOURCE_CODE
    , obs.SOURCE_DESCRIPTION
    , case
        when obs.NORMALIZED_CODE_TYPE is not null then obs.NORMALIZED_CODE_TYPE
        when icd10cm.icd_10_cm is not null then 'icd-10-cm'
        when icd9cm.icd_9_cm is not null then 'icd-9-cm'
        when icd10pcs.icd_10_pcs is not null then 'icd-10-pcs'
        when icd9pcs.icd_9_pcs is not null then 'icd-10-pcs'
        when hcpcs.hcpcs is not null then 'hcpcs'
        when snomed_ct.snomed_ct is not null then 'snomed-ct'
        when loinc.loinc is not null then 'loinc'
        end as NORMALIZED_CODE_TYPE
  , coalesce(
        obs.NORMALIZED_CODE
      , icd10cm.icd_10_cm
      , icd9cm.icd_9_cm
      , icd10pcs.icd_10_pcs
      , icd9pcs.icd_9_pcs
      , hcpcs.hcpcs
      , snomed_ct.snomed_ct
      , loinc.loinc
      ) as NORMALIZED_CODE
      , coalesce(
        obs.NORMALIZED_DESCRIPTION
      , icd10cm.short_description
      , icd9cm.short_description
      , icd10pcs.description
      , icd9pcs.short_description
      , hcpcs.short_description
      , snomed_ct.description
      , loinc.long_common_name
      ) as NORMALIZED_DESCRIPTION
     , case
         when coalesce(obs.NORMALIZED_CODE, obs.NORMALIZED_DESCRIPTION) is not null then 'manual'
         when coalesce(
            icd10cm.icd_10_cm
          , icd9cm.icd_9_cm
          , icd10pcs.icd_10_pcs
          , icd9pcs.icd_9_pcs
          , hcpcs.hcpcs
          , snomed_ct.snomed_ct
          , loinc.loinc) is not null then 'automatic'
         end as mapping_method
    , obs.RESULT
    , obs.SOURCE_UNITS
    , obs.NORMALIZED_UNITS
    , obs.SOURCE_REFERENCE_RANGE_LOW
    , obs.SOURCE_REFERENCE_RANGE_HIGH
    , obs.NORMALIZED_REFERENCE_RANGE_LOW
    , obs.NORMALIZED_REFERENCE_RANGE_HIGH
    , obs.DATA_SOURCE
    , obs.TUVA_LAST_RUN
from {{ ref('core__stg_clinical_observation')}} obs
left join {{ ref('terminology__icd_10_cm') }} icd10cm
    on obs.source_code_type = 'icd-10-cm'
        and replace(obs.source_code,'.','') = icd10cm.icd_10_cm
left join {{ ref('terminology__icd_9_cm') }} icd9cm
    on obs.source_code_type = 'icd-9-cm'
        and replace(obs.source_code,'.','') = icd9cm.icd_9_cm
left join {{ ref('terminology__icd_10_pcs') }} icd10pcs
    on obs.source_code_type = 'icd-10-pcs'
        and obs.source_code = icd10pcs.icd_10_pcs
left join {{ ref('terminology__icd_9_pcs') }} icd9pcs
    on obs.source_code_type = 'icd-9-pcs'
        and replace(obs.source_code,'.','') = icd9pcs.icd_9_pcs
left join {{ ref('terminology__hcpcs_level_2') }} hcpcs
    on obs.source_code_type = 'hcpcs'
        and obs.source_code = hcpcs.hcpcs
left join {{ ref('terminology__snomed_ct')}} snomed_ct
    on obs.source_code_type = 'snomed-ct'
        and obs.source_code = snomed_ct.snomed_ct
left join {{ ref('terminology__loinc') }} loinc
    on obs.source_code_type = 'loinc'
        and obs.source_code = loinc.loinc

{% else %}

select
      obs.OBSERVATION_ID
    , obs.PATIENT_ID
    , obs.ENCOUNTER_ID
    , obs.PANEL_ID
    , obs.OBSERVATION_DATE
    , obs.OBSERVATION_TYPE
    , obs.SOURCE_CODE_TYPE
    , obs.SOURCE_CODE
    , obs.SOURCE_DESCRIPTION
    , case
        when obs.NORMALIZED_CODE_TYPE is not null then obs.NORMALIZED_CODE_TYPE
        when icd10cm.icd_10_cm is not null then 'icd-10-cm'
        when icd9cm.icd_9_cm is not null then 'icd-9-cm'
        when icd10pcs.icd_10_pcs is not null then 'icd-10-pcs'
        when icd9pcs.icd_9_pcs is not null then 'icd-10-pcs'
        when hcpcs.hcpcs is not null then 'hcpcs'
        when snomed_ct.snomed_ct is not null then 'snomed-ct'
        when loinc.loinc is not null then 'loinc'
        else custom_mapped.normalized_code_type end as NORMALIZED_CODE_TYPE
   , coalesce(
        obs.NORMALIZED_CODE
      , icd10cm.icd_10_cm
      , icd9cm.icd_9_cm
      , icd10pcs.icd_10_pcs
      , icd9pcs.icd_9_pcs
      , hcpcs.hcpcs
      , snomed_ct.snomed_ct
      , loinc.loinc
      , custom_mapped.normalized_code
      ) as NORMALIZED_CODE
   , coalesce(
        obs.NORMALIZED_DESCRIPTION
      , icd10cm.short_description
      , icd9cm.short_description
      , icd10pcs.description
      , icd9pcs.short_description
      , hcpcs.short_description
      , snomed_ct.description
      , loinc.long_common_name
      , custom_mapped.normalized_description
      ) as NORMALIZED_DESCRIPTION
   , case
         when coalesce(obs.NORMALIZED_CODE, obs.NORMALIZED_DESCRIPTION) is not null then 'manual'
         when coalesce(
            icd10cm.icd_10_cm
          , icd9cm.icd_9_cm
          , icd10pcs.icd_10_pcs
          , icd9pcs.icd_9_pcs
          , hcpcs.hcpcs
          , snomed_ct.snomed_ct
          , loinc.loinc) is not null then 'automatic'
         when custom_mapped.not_mapped is not null then custom_mapped.not_mapped
         when coalesce(custom_mapped.normalized_code,custom_mapped.normalized_description) is not null then 'custom'
         end as mapping_method
    , obs.RESULT
    , obs.SOURCE_UNITS
    , obs.NORMALIZED_UNITS
    , obs.SOURCE_REFERENCE_RANGE_LOW
    , obs.SOURCE_REFERENCE_RANGE_HIGH
    , obs.NORMALIZED_REFERENCE_RANGE_LOW
    , obs.NORMALIZED_REFERENCE_RANGE_HIGH
    , obs.DATA_SOURCE
    , obs.TUVA_LAST_RUN
from {{ ref('core__stg_clinical_observation')}} obs
left join {{ ref('terminology__icd_10_cm') }} icd10cm
    on obs.source_code_type = 'icd-10-cm'
        and replace(obs.source_code,'.','') = icd10cm.icd_10_cm
left join {{ ref('terminology__icd_9_cm') }} icd9cm
    on obs.source_code_type = 'icd-9-cm'
        and replace(obs.source_code,'.','') = icd9cm.icd_9_cm
left join {{ ref('terminology__icd_10_pcs') }} icd10pcs
    on obs.source_code_type = 'icd-10-pcs'
        and obs.source_code = icd10pcs.icd_10_pcs
left join {{ ref('terminology__icd_9_pcs') }} icd9pcs
    on obs.source_code_type = 'icd-9-pcs'
        and replace(obs.source_code,'.','') = icd9pcs.icd_9_pcs
left join {{ ref('terminology__hcpcs_level_2') }} hcpcs
    on obs.source_code_type = 'hcpcs'
        and obs.source_code = hcpcs.hcpcs
left join {{ ref('terminology__snomed_ct')}} snomed_ct
    on obs.source_code_type = 'snomed-ct'
        and obs.source_code = snomed_ct.snomed_ct
left join {{ ref('terminology__loinc') }} loinc
    on obs.source_code_type = 'loinc'
        and obs.source_code = loinc.loinc
left join {{ ref('custom_mapped') }} custom_mapped
    on  ( lower(obs.source_code_type) = lower(custom_mapped.source_code_type)
        or ( obs.source_code_type is null and custom_mapped.source_code_type is null)
        )
    and (obs.source_code = custom_mapped.source_code
        or ( obs.source_code is null and custom_mapped.source_code is null)
        )
    and (obs.source_description = custom_mapped.source_description
        or ( obs.source_description is null and custom_mapped.source_description is null)
        )
    and not (obs.source_code is null and obs.source_description is null)
{% endif %}