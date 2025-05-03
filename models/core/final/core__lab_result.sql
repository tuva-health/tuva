{{ config(
     enabled = var('clinical_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

{% if var('enable_normalize_engine',false) != true %}


select
      labs.lab_result_id
    , labs.person_id
    , labs.patient_id
    , labs.encounter_id
    , labs.accession_number
    , labs.source_code_type
    , labs.source_code
    , labs.source_description
    , labs.source_component
    , case
        when labs.normalized_code_type is not null then labs.normalized_code_type
        when loinc.loinc is not null then 'loinc'
        when snomed_ct.snomed_ct is not null then 'snomed-ct'
        else null end as normalized_code_type
    , coalesce(
        labs.normalized_code
        , loinc.loinc
        , snomed_ct.snomed_ct
        ) as normalized_code
    , coalesce(
        labs.normalized_description
        , loinc.long_common_name
        , snomed_ct.description
        ) as normalized_description
    , case when coalesce(labs.normalized_code, labs.normalized_description) is not null then 'manual'
         when coalesce(loinc.loinc, snomed_ct.snomed_ct) is not null then 'automatic'
         end as mapping_method
    , labs.normalized_component
    , labs.status
    , labs.result
    , labs.result_date
    , labs.collection_date
    , labs.source_units
    , labs.normalized_units
    , labs.source_reference_range_low
    , labs.source_reference_range_high
    , labs.normalized_reference_range_low
    , labs.normalized_reference_range_high
    , labs.source_abnormal_flag
    , labs.normalized_abnormal_flag
    , labs.specimen
    , labs.ordering_practitioner_id
    , labs.data_source
    , labs.tuva_last_run
from {{ ref('core__stg_clinical_lab_result') }} as labs
left outer join {{ ref('terminology__loinc') }} as loinc
    on labs.source_code_type = 'loinc'
        and labs.source_code = loinc.loinc
left outer join {{ ref('terminology__snomed_ct') }} as snomed_ct
    on labs.source_code_type = 'snomed-ct'
        and labs.source_code = snomed_ct.snomed_ct

 {% else %}

select
      labs.lab_result_id
    , labs.person_id
    , labs.patient_id
    , labs.encounter_id
    , labs.accession_number
    , labs.source_code_type
    , labs.source_code
    , labs.source_description
    , labs.source_component
    , case
        when labs.normalized_code_type is not null then labs.normalized_code_type
        when loinc.loinc is not null then 'loinc'
        when snomed_ct.snomed_ct is not null then 'snomed-ct'
        else custom_mapped.normalized_code_type end as normalized_code_type
    , coalesce(
        labs.normalized_code
        , loinc.loinc
        , snomed_ct.snomed_ct
        , custom_mapped.normalized_code
        ) as normalized_code
    , coalesce(
        labs.normalized_description
        , loinc.long_common_name
        , snomed_ct.description
        , custom_mapped.normalized_description
        ) normalized_description
  , case  when coalesce(labs.normalized_code, labs.normalized_description) is not null then 'manual'
        when coalesce(loinc.loinc,snomed_ct.snomed_ct) is not null then 'automatic'
        when custom_mapped.not_mapped is not null then custom_mapped.not_mapped
        when coalesce(custom_mapped.normalized_code,custom_mapped.normalized_description) is not null then 'custom'
        end as mapping_method
    , labs.normalized_component
    , labs.status
    , labs.result
    , labs.result_date
    , labs.collection_date
    , labs.source_units
    , labs.normalized_units
    , labs.source_reference_range_low
    , labs.source_reference_range_high
    , labs.normalized_reference_range_low
    , labs.normalized_reference_range_high
    , labs.source_abnormal_flag
    , labs.normalized_abnormal_flag
    , labs.specimen
    , labs.ordering_practitioner_id
    , labs.data_source
    , labs.tuva_last_run
From  {{ ref('core__stg_clinical_lab_result') }} as labs
left join {{ ref('terminology__loinc') }} loinc
    on labs.source_code_type = 'loinc'
        and labs.source_code = loinc.loinc
left join {{ ref('terminology__snomed_ct') }} snomed_ct
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
