{{ config(
     enabled = var('clinical_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

{% if var('enable_normalize_engine',false) != true %}
select
      obs.observation_id
    , obs.person_id
    , obs.patient_id
    , obs.encounter_id
    , obs.panel_id
    , obs.observation_date
    , obs.observation_type
    , obs.source_code_type
    , obs.source_code
    , obs.source_description
    , case
        when obs.normalized_code_type is not null then obs.normalized_code_type
        when icd10cm.icd_10_cm is not null then 'icd-10-cm'
        when icd9cm.icd_9_cm is not null then 'icd-9-cm'
        when icd10pcs.icd_10_pcs is not null then 'icd-10-pcs'
        when icd9pcs.icd_9_pcs is not null then 'icd-10-pcs'
        when hcpcs.hcpcs is not null then 'hcpcs'
        when snomed_ct.snomed_ct is not null then 'snomed-ct'
        when loinc.loinc is not null then 'loinc'
        end as normalized_code_type
  , coalesce(
        obs.normalized_code
      , icd10cm.icd_10_cm
      , icd9cm.icd_9_cm
      , icd10pcs.icd_10_pcs
      , icd9pcs.icd_9_pcs
      , hcpcs.hcpcs
      , snomed_ct.snomed_ct
      , loinc.loinc
      ) as normalized_code
      , coalesce(
        obs.normalized_description
      , icd10cm.short_description
      , icd9cm.short_description
      , icd10pcs.description
      , icd9pcs.short_description
      , hcpcs.short_description
      , snomed_ct.description
      , loinc.long_common_name
      ) as normalized_description
     , case
         when coalesce(obs.normalized_code, obs.normalized_description) is not null then 'manual'
         when coalesce(
            icd10cm.icd_10_cm
          , icd9cm.icd_9_cm
          , icd10pcs.icd_10_pcs
          , icd9pcs.icd_9_pcs
          , hcpcs.hcpcs
          , snomed_ct.snomed_ct
          , loinc.loinc) is not null then 'automatic'
         end as mapping_method
    , obs.result
    , obs.source_units
    , obs.normalized_units
    , obs.source_reference_range_low
    , obs.source_reference_range_high
    , obs.normalized_reference_range_low
    , obs.normalized_reference_range_high
    , obs.data_source
    , obs.tuva_last_run
from {{ ref('core__stg_clinical_observation') }} as obs
left outer join {{ ref('terminology__icd_10_cm') }} as icd10cm
    on obs.source_code_type = 'icd-10-cm'
        and replace(obs.source_code, '.', '') = icd10cm.icd_10_cm
left outer join {{ ref('terminology__icd_9_cm') }} as icd9cm
    on obs.source_code_type = 'icd-9-cm'
        and replace(obs.source_code, '.', '') = icd9cm.icd_9_cm
left outer join {{ ref('terminology__icd_10_pcs') }} as icd10pcs
    on obs.source_code_type = 'icd-10-pcs'
        and obs.source_code = icd10pcs.icd_10_pcs
left outer join {{ ref('terminology__icd_9_pcs') }} as icd9pcs
    on obs.source_code_type = 'icd-9-pcs'
        and replace(obs.source_code, '.', '') = icd9pcs.icd_9_pcs
left outer join {{ ref('terminology__hcpcs_level_2') }} as hcpcs
    on obs.source_code_type = 'hcpcs'
        and obs.source_code = hcpcs.hcpcs
left outer join {{ ref('terminology__snomed_ct') }} as snomed_ct
    on obs.source_code_type = 'snomed-ct'
        and obs.source_code = snomed_ct.snomed_ct
left outer join {{ ref('terminology__loinc') }} as loinc
    on obs.source_code_type = 'loinc'
        and obs.source_code = loinc.loinc

{% else %}

select
      obs.observation_id
    , obs.person_id
    , obs.patient_id
    , obs.encounter_id
    , obs.panel_id
    , obs.observation_date
    , obs.observation_type
    , obs.source_code_type
    , obs.source_code
    , obs.source_description
    , case
        when obs.normalized_code_type is not null then obs.normalized_code_type
        when icd10cm.icd_10_cm is not null then 'icd-10-cm'
        when icd9cm.icd_9_cm is not null then 'icd-9-cm'
        when icd10pcs.icd_10_pcs is not null then 'icd-10-pcs'
        when icd9pcs.icd_9_pcs is not null then 'icd-10-pcs'
        when hcpcs.hcpcs is not null then 'hcpcs'
        when snomed_ct.snomed_ct is not null then 'snomed-ct'
        when loinc.loinc is not null then 'loinc'
        else custom_mapped.normalized_code_type end as normalized_code_type
   , coalesce(
        obs.normalized_code
      , icd10cm.icd_10_cm
      , icd9cm.icd_9_cm
      , icd10pcs.icd_10_pcs
      , icd9pcs.icd_9_pcs
      , hcpcs.hcpcs
      , snomed_ct.snomed_ct
      , loinc.loinc
      , custom_mapped.normalized_code
      ) as normalized_code
   , coalesce(
        obs.normalized_description
      , icd10cm.short_description
      , icd9cm.short_description
      , icd10pcs.description
      , icd9pcs.short_description
      , hcpcs.short_description
      , snomed_ct.description
      , loinc.long_common_name
      , custom_mapped.normalized_description
      ) as normalized_description
   , case
         when coalesce(obs.normalized_code, obs.normalized_description) is not null then 'manual'
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
    , obs.result
    , obs.source_units
    , obs.normalized_units
    , obs.source_reference_range_low
    , obs.source_reference_range_high
    , obs.normalized_reference_range_low
    , obs.normalized_reference_range_high
    , obs.data_source
    , obs.tuva_last_run
from {{ ref('core__stg_clinical_observation') }} obs
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
left join {{ ref('terminology__snomed_ct') }} snomed_ct
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
