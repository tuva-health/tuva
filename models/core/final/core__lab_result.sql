{{ config(
     enabled = var('clinical_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

{% if var('enable_normalize_engine',false) != true %}


select
      labs.LAB_RESULT_ID
    , labs.PATIENT_ID
    , labs.ENCOUNTER_ID
    , labs.ACCESSION_NUMBER
    , labs.SOURCE_CODE_TYPE
    , labs.SOURCE_CODE
    , labs.SOURCE_DESCRIPTION
    , labs.SOURCE_COMPONENT
    , case
        when labs.normalized_code_type is not null then labs.normalized_code_type
        when loinc.loinc is not null then 'loinc'
        when snomed_ct.snomed_ct is not null then 'snomed-ct'
        else null end as NORMALIZED_CODE_TYPE
    , coalesce(
        labs.normalized_code
        , loinc.loinc
        , snomed_ct.snomed_ct
        ) as NORMALIZED_CODE
    , coalesce(
        labs.normalized_description
        , loinc.long_common_name
        , snomed_ct.description
        ) NORMALIZED_DESCRIPTION
    , case when coalesce(labs.NORMALIZED_CODE, labs.NORMALIZED_DESCRIPTION) is not null then 'manual'
         when coalesce(LOINC.loinc,snomed_ct.snomed_ct) is not null then 'automatic'
         end as mapping_method
    , labs.NORMALIZED_COMPONENT
    , labs.STATUS
    , labs.RESULT
    , labs.RESULT_DATE
    , labs.COLLECTION_DATE
    , labs.SOURCE_UNITS
    , labs.NORMALIZED_UNITS
    , labs.SOURCE_REFERENCE_RANGE_LOW
    , labs.SOURCE_REFERENCE_RANGE_HIGH
    , labs.NORMALIZED_REFERENCE_RANGE_LOW
    , labs.NORMALIZED_REFERENCE_RANGE_HIGH
    , labs.SOURCE_ABNORMAL_FLAG
    , labs.NORMALIZED_ABNORMAL_FLAG
    , labs.SPECIMEN
    , labs.ORDERING_PRACTITIONER_ID
    , labs.DATA_SOURCE
    , labs.TUVA_LAST_RUN
From {{ ref('core__stg_clinical_lab_result')}} as labs
left join {{ ref('terminology__loinc') }} loinc
    on labs.source_code_type = 'loinc'
        and labs.source_code = loinc.loinc
left join {{ref('terminology__snomed_ct')}} snomed_ct
    on labs.source_code_type = 'snomed-ct'
        and labs.source_code = snomed_ct.snomed_ct

 {% else %}

select
      labs.LAB_RESULT_ID
    , labs.PATIENT_ID
    , labs.ENCOUNTER_ID
    , labs.ACCESSION_NUMBER
    , labs.SOURCE_CODE_TYPE
    , labs.SOURCE_CODE
    , labs.SOURCE_DESCRIPTION
    , labs.SOURCE_COMPONENT
    , case
        when labs.NORMALIZED_CODE_TYPE is not null then labs.NORMALIZED_CODE_TYPE
        when loinc.loinc is not null then 'loinc'
        when snomed_ct.snomed_ct is not null then 'snomed-ct'
        else custom_mapped.normalized_code_type end as NORMALIZED_CODE_TYPE
    , coalesce(
        labs.normalized_code
        , loinc.loinc
        , snomed_ct.snomed_ct
        , custom_mapped.normalized_code
        ) as NORMALIZED_CODE
    , coalesce(
        labs.normalized_description
        , loinc.long_common_name
        , snomed_ct.description
        , custom_mapped.normalized_description
        ) NORMALIZED_DESCRIPTION
  , case  when coalesce(labs.NORMALIZED_CODE, labs.NORMALIZED_DESCRIPTION) is not null then 'manual'
        when coalesce(LOINC.loinc,snomed_ct.snomed_ct) is not null then 'automatic'
        when custom_mapped.not_mapped is not null then custom_mapped.not_mapped
        when coalesce(custom_mapped.normalized_code,custom_mapped.normalized_description) is not null then 'custom'
        end as mapping_method
    , labs.NORMALIZED_COMPONENT
    , labs.STATUS
    , labs.RESULT
    , labs.RESULT_DATE
    , labs.COLLECTION_DATE
    , labs.SOURCE_UNITS
    , labs.NORMALIZED_UNITS
    , labs.SOURCE_REFERENCE_RANGE_LOW
    , labs.SOURCE_REFERENCE_RANGE_HIGH
    , labs.NORMALIZED_REFERENCE_RANGE_LOW
    , labs.NORMALIZED_REFERENCE_RANGE_HIGH
    , labs.SOURCE_ABNORMAL_FLAG
    , labs.NORMALIZED_ABNORMAL_FLAG
    , labs.SPECIMEN
    , labs.ORDERING_PRACTITIONER_ID
    , labs.DATA_SOURCE
    , labs.TUVA_LAST_RUN
From  {{ ref('core__stg_clinical_lab_result')}} as labs
left join {{ ref('terminology__loinc') }} loinc
    on labs.source_code_type = 'loinc'
        and labs.source_code = loinc.loinc
left join {{ref('terminology__snomed_ct')}} snomed_ct
    on labs.source_code_type = 'snomed-ct'
        and labs.source_code = snomed_ct.snomed_ct
left join {{ ref('custom_mapped') }} custom_mapped
    on  ( lower(labs.source_code_type) = lower(custom_mapped.source_code_type)
        or ( labs.source_code_type is null and custom_mapped.source_code_type is null)
        )
    and (labs.source_code = custom_mapped.source_code
        or ( labs.source_code is null and custom_mapped.source_code is null)
        )
    and (labs.source_description = custom_mapped.source_description
        or ( labs.source_description is null and custom_mapped.source_description is null)
        )
    and not (labs.source_code is null and labs.source_description is null)
{% endif %}