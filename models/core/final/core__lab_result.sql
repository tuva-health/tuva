{{ config(
     enabled = var('clinical_enabled', False)
 | as_bool
   )
}}

{%- set tuva_extension_columns -%}
    {{ select_extension_columns(ref('input_layer__lab_result')) }}
{%- endset -%}

{%- set tuva_metadata_columns -%}
    , labs.tuva_last_run
    , labs.data_source
{%- endset -%}

{% if var('enable_normalize_engine',false) != true %}

select
      labs.lab_result_id
    , labs.person_id
    , labs.patient_id
    , labs.encounter_id
    , labs.accession_number
    , labs.source_order_type
    , labs.source_order_code
    , labs.source_order_description
    , labs.source_component_type
    , labs.source_component_code
    , labs.source_component_description
    , case
        when lower(labs.source_order_type) = 'loinc'
            and loinc.loinc is not null then 'loinc'
        when lower(labs.source_order_type) = 'snomed-ct'
            and snomed_ct.snomed_ct is not null then 'snomed-ct'
        else null
      end as normalized_order_type
    , coalesce(
        case
            when lower(labs.source_order_type) = 'loinc' then loinc.loinc
        end
        , case
            when lower(labs.source_order_type) = 'snomed-ct' then snomed_ct.snomed_ct
        end
      ) as normalized_order_code
    , coalesce(
        case
            when lower(labs.source_order_type) = 'loinc' then loinc.long_common_name
        end
        , case
            when lower(labs.source_order_type) = 'snomed-ct' then snomed_ct.description
        end
      ) as normalized_order_description
    , case
        when lower(labs.source_component_type) = 'loinc'
            and loinc_component.loinc is not null then 'loinc'
        when lower(labs.source_component_type) = 'snomed-ct'
            and snomed_ct_component.snomed_ct is not null then 'snomed-ct'
        else null
      end as normalized_component_type
    , coalesce(
        case
            when lower(labs.source_component_type) = 'loinc' then loinc_component.loinc
        end
        , case
            when lower(labs.source_component_type) = 'snomed-ct' then snomed_ct_component.snomed_ct
        end
      ) as normalized_component_code
    , coalesce(
        case
            when lower(labs.source_component_type) = 'loinc' then loinc_component.long_common_name
        end
        , case
            when lower(labs.source_component_type) = 'snomed-ct' then snomed_ct_component.description
        end
      ) as normalized_component_description
    , case
        when coalesce(
              case
                when lower(labs.source_order_type) = 'loinc' then loinc.loinc
              end
            , case
                when lower(labs.source_order_type) = 'snomed-ct' then snomed_ct.snomed_ct
              end
            , case
                when lower(labs.source_component_type) = 'loinc' then loinc_component.loinc
              end
            , case
                when lower(labs.source_component_type) = 'snomed-ct' then snomed_ct_component.snomed_ct
              end
        ) is not null then 'automatic'
      end as mapping_method
    , labs.status
    , labs.result
    , labs.result_datetime
    , labs.collection_datetime
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
    {{ tuva_extension_columns }}
    {{ tuva_metadata_columns }}
from {{ ref('core__stg_clinical_lab_result') }} as labs
    left join {{ ref('terminology__loinc') }} as loinc
        on labs.source_order_code = loinc.loinc
    left join {{ ref('terminology__snomed_ct') }} as snomed_ct
        on labs.source_order_code = snomed_ct.snomed_ct
    left join {{ ref('terminology__loinc') }} as loinc_component
        on labs.source_component_code = loinc_component.loinc
    left join {{ ref('terminology__snomed_ct') }} as snomed_ct_component
        on labs.source_component_code = snomed_ct_component.snomed_ct

 {% else %}

select
      labs.lab_result_id
    , labs.person_id
    , labs.patient_id
    , labs.encounter_id
    , labs.accession_number
    , labs.source_order_type
    , labs.source_order_code
    , labs.source_order_description
    , labs.source_component_type
    , labs.source_component_code
    , labs.source_component_description
    , case
        when lower(labs.source_order_type) = 'loinc'
            and loinc.loinc is not null then 'loinc'
        when lower(labs.source_order_type) = 'snomed-ct'
            and snomed_ct.snomed_ct is not null then 'snomed-ct'
        else custom_mapped_order.normalized_code_type
      end as normalized_order_type
    , coalesce(
        case
            when lower(labs.source_order_type) = 'loinc' then loinc.loinc
        end
        , case
            when lower(labs.source_order_type) = 'snomed-ct' then snomed_ct.snomed_ct
        end
        , custom_mapped_order.normalized_code
      ) as normalized_order_code
    , coalesce(
        case
            when lower(labs.source_order_type) = 'loinc' then loinc.long_common_name
        end
        , case
            when lower(labs.source_order_type) = 'snomed-ct' then snomed_ct.description
        end
        , custom_mapped_order.normalized_description
      ) as normalized_order_description
    , case
        when lower(labs.source_component_type) = 'loinc'
            and loinc_component.loinc is not null then 'loinc'
        when lower(labs.source_component_type) = 'snomed-ct'
            and snomed_ct_component.snomed_ct is not null then 'snomed-ct'
        else custom_mapped_component.normalized_code_type
      end as normalized_component_type
    , coalesce(
        case
            when lower(labs.source_component_type) = 'loinc' then loinc_component.loinc
        end
        , case
            when lower(labs.source_component_type) = 'snomed-ct' then snomed_ct_component.snomed_ct
        end
        , custom_mapped_component.normalized_code
      ) as normalized_component_code
    , coalesce(
        case
            when lower(labs.source_component_type) = 'loinc' then loinc_component.long_common_name
        end
        , case
            when lower(labs.source_component_type) = 'snomed-ct' then snomed_ct_component.description
        end
        , custom_mapped_component.normalized_description
      ) as normalized_component_description
    , case
        when coalesce(
              case
                when lower(labs.source_order_type) = 'loinc' then loinc.loinc
              end
            , case
                when lower(labs.source_order_type) = 'snomed-ct' then snomed_ct.snomed_ct
              end
            , case
                when lower(labs.source_component_type) = 'loinc' then loinc_component.loinc
              end
            , case
                when lower(labs.source_component_type) = 'snomed-ct' then snomed_ct_component.snomed_ct
              end
        ) is not null then 'automatic'
        when coalesce(
              custom_mapped_order.not_mapped
            , custom_mapped_component.not_mapped
        ) is not null then coalesce(
              custom_mapped_order.not_mapped
            , custom_mapped_component.not_mapped
        )
        when coalesce(
              custom_mapped_order.normalized_code
            , custom_mapped_order.normalized_description
            , custom_mapped_component.normalized_code
            , custom_mapped_component.normalized_description
        ) is not null then 'custom'
      end as mapping_method
    , labs.status
    , labs.result
    , labs.result_datetime
    , labs.collection_datetime
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
    {{ tuva_extension_columns }}
    {{ tuva_metadata_columns }}
from {{ ref('core__stg_clinical_lab_result') }} as labs
    left join {{ ref('terminology__loinc') }} as loinc
        on labs.source_order_code = loinc.loinc
    left join {{ ref('terminology__snomed_ct') }} as snomed_ct
        on labs.source_order_code = snomed_ct.snomed_ct
    left join {{ ref('terminology__loinc') }} as loinc_component
        on labs.source_component_code = loinc_component.loinc
    left join {{ ref('terminology__snomed_ct') }} as snomed_ct_component
        on labs.source_component_code = snomed_ct_component.snomed_ct
    left join {{ ref('custom_mapped') }} as custom_mapped_order
        on (lower(labs.source_order_type) = lower(custom_mapped_order.source_code_type)
            or (labs.source_order_type is null and custom_mapped_order.source_code_type is null)
        )
        and (labs.source_order_code = custom_mapped_order.source_code
            or (labs.source_order_code is null and custom_mapped_order.source_code is null)
        )
        and (labs.source_order_description = custom_mapped_order.source_description
            or (labs.source_order_description is null and custom_mapped_order.source_description is null)
        )
        and not (labs.source_order_code is null and labs.source_order_description is null)
    left join {{ ref('custom_mapped') }} as custom_mapped_component
        on (lower(labs.source_component_type) = lower(custom_mapped_component.source_code_type)
            or (labs.source_component_type is null and custom_mapped_component.source_code_type is null)
        )
        and (labs.source_component_code = custom_mapped_component.source_code
            or (labs.source_component_code is null and custom_mapped_component.source_code is null)
        )
        and (labs.source_component_description = custom_mapped_component.source_description
            or (labs.source_component_description is null and custom_mapped_component.source_description is null)
        )
        and not (labs.source_component_code is null and labs.source_component_description is null)
{% endif %}
