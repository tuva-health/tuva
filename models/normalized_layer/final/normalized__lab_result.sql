{{ config(
     enabled = the_tuva_project.tuva_boolean_var('clinical_enabled', false)
   )
}}

{%- set tuva_core_columns -%}
      cast(lab_result_id as {{ dbt.type_string() }}) as lab_result_id
    , cast(person_id as {{ dbt.type_string() }}) as person_id
    , cast(patient_id as {{ dbt.type_string() }}) as patient_id
    , cast(encounter_id as {{ dbt.type_string() }}) as encounter_id
    , cast(accession_number as {{ dbt.type_string() }}) as accession_number
    , cast(source_order_type as {{ dbt.type_string() }}) as source_order_type
    , cast(source_order_code as {{ dbt.type_string() }}) as source_order_code
    , cast(source_order_description as {{ dbt.type_string() }}) as source_order_description
    , cast(source_component_type as {{ dbt.type_string() }}) as source_component_type
    , cast(source_component_code as {{ dbt.type_string() }}) as source_component_code
    , cast(source_component_description as {{ dbt.type_string() }}) as source_component_description
    , cast(status as {{ dbt.type_string() }}) as status
    , cast(result as {{ dbt.type_string() }}) as result
    , {{ try_to_cast_datetime('result_datetime') }} as result_datetime
    , {{ try_to_cast_datetime('collection_datetime') }} as collection_datetime
    , cast(source_units as {{ dbt.type_string() }}) as source_units
    , cast(normalized_units as {{ dbt.type_string() }}) as normalized_units
    , cast(source_reference_range_low as {{ dbt.type_string() }}) as source_reference_range_low
    , cast(source_reference_range_high as {{ dbt.type_string() }}) as source_reference_range_high
    , cast(normalized_reference_range_low as {{ dbt.type_string() }}) as normalized_reference_range_low
    , cast(normalized_reference_range_high as {{ dbt.type_string() }}) as normalized_reference_range_high
    , cast(source_abnormal_code as {{ dbt.type_string() }}) as source_abnormal_code
    , cast(normalized_abnormal_code as {{ dbt.type_string() }}) as normalized_abnormal_code
    , cast(specimen as {{ dbt.type_string() }}) as specimen
    , cast(ordering_practitioner_id as {{ dbt.type_string() }}) as ordering_practitioner_id
{%- endset -%}

{%- set tuva_metadata_columns -%}
    , cast(ingest_datetime as {{ dbt.type_timestamp() }}) as ingest_datetime
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
    , cast(data_source as {{ dbt.type_string() }}) as data_source
{%- endset %}

{%- set tuva_extension_columns -%}
    {{ select_extension_columns(ref('input_layer__lab_result'), strip_prefix=false) }}
{%- endset %}

with labs as (

    select
        {{ tuva_core_columns }}
        {{ tuva_extension_columns }}
        {{ tuva_metadata_columns }}
    from {{ ref('input_layer__lab_result') }}

)

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
    , labs.source_abnormal_code
    , labs.normalized_abnormal_code
    , labs.specimen
    , labs.ordering_practitioner_id
    {{ select_extension_columns(ref('input_layer__lab_result'), alias='labs', strip_prefix=false) }}
    , labs.ingest_datetime
    , labs.tuva_last_run
    , labs.data_source
from labs
left join {{ ref('terminology__loinc') }} as loinc
    on labs.source_order_code = loinc.loinc
left join {{ ref('terminology__snomed_ct') }} as snomed_ct
    on labs.source_order_code = snomed_ct.snomed_ct
left join {{ ref('terminology__loinc') }} as loinc_component
    on labs.source_component_code = loinc_component.loinc
left join {{ ref('terminology__snomed_ct') }} as snomed_ct_component
    on labs.source_component_code = snomed_ct_component.snomed_ct
