{{ config(
     enabled = (var('enable_data_quality', false) | as_bool) and (var('clinical_enabled', false) | as_bool),
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'medication_flags',
     tags = ['data_quality', 'dq', 'dq1', 'dq_logical'],
     materialized = 'table'
   )
}}

{% set string_type = dbt.type_string() %}
{% set current_date_sql = dq_current_date_sql() %}
{% set min_event_date_sql = dq_date_literal_sql('2000-01-01') %}
{% set source_code_type_valid_where = "lower(cast(source_rows.source_code_type as " ~ string_type ~ ")) in ('ndc', 'rxnorm', 'atc', 'local', 'unknown')" %}

with source_rows as (
    select *
    from {{ ref('input_layer__medication') }}
),

patient_rows as (
    select distinct
          person_id
        , patient_id
        , data_source
    from {{ ref('input_layer__patient') }}
),

encounter_rows as (
    select distinct
          encounter_id
        , data_source
    from {{ ref('input_layer__encounter') }}
),

practitioner_rows as (
    select distinct
          practitioner_id
        , data_source
    from {{ ref('input_layer__practitioner') }}
),

ndc_rows as (
    select distinct
          ndc_terminology.ndc
    from {{ ref('terminology__ndc') }} as ndc_terminology
),

rxnorm_rows as (
    select distinct
          rxcui
    from {{ ref('terminology__rxnorm_to_atc') }}
),

atc_rows as (
    select distinct atc_code
    from (
        select atc_1_code as atc_code from {{ ref('terminology__rxnorm_to_atc') }}
        union all
        select atc_2_code as atc_code from {{ ref('terminology__rxnorm_to_atc') }}
        union all
        select atc_3_code as atc_code from {{ ref('terminology__rxnorm_to_atc') }}
        union all
        select atc_4_code as atc_code from {{ ref('terminology__rxnorm_to_atc') }}
    ) as atc_union
    where atc_code is not null
),

final as (
    select
          source_rows.medication_id
        , source_rows.person_id
        , source_rows.patient_id
        , source_rows.encounter_id
        , source_rows.practitioner_id
        , source_rows.dispensing_date
        , source_rows.prescribing_date
        , source_rows.source_code_type
        , source_rows.source_code
        , source_rows.ndc_code
        , source_rows.rxnorm_code
        , source_rows.atc_code
        , source_rows.quantity
        , source_rows.days_supply
        , source_rows.data_source
        , {{ dq_logical_int_flag_sql("source_rows.person_id is null") }} as person_id_null
        , {{ dq_logical_int_flag_sql("source_rows.patient_id is null") }} as patient_id_null
        , {{ dq_logical_int_flag_sql("source_rows.person_id is not null and patient_person.person_id is null") }} as person_id_not_in_patient
        , {{ dq_logical_int_flag_sql("source_rows.patient_id is not null and patient_patient.patient_id is null") }} as patient_id_not_in_patient
        , {{ dq_logical_int_flag_sql("source_rows.encounter_id is not null and encounter_rows.encounter_id is null") }} as encounter_id_not_in_encounter
        , {{ dq_logical_int_flag_sql("source_rows.practitioner_id is not null and practitioner_rows.practitioner_id is null") }} as practitioner_id_not_in_practitioner
        , {{ dq_logical_int_flag_sql("source_rows.dispensing_date is not null and (source_rows.dispensing_date < " ~ min_event_date_sql ~ " or source_rows.dispensing_date > " ~ current_date_sql ~ ")") }} as dispensing_date_out_of_range
        , {{ dq_logical_int_flag_sql("source_rows.prescribing_date is not null and (source_rows.prescribing_date < " ~ min_event_date_sql ~ " or source_rows.prescribing_date > " ~ current_date_sql ~ ")") }} as prescribing_date_out_of_range
        , {{ dq_logical_int_flag_sql("source_rows.prescribing_date is not null and source_rows.dispensing_date is not null and source_rows.prescribing_date > source_rows.dispensing_date") }} as prescribing_date_after_dispensing_date
        , {{ dq_logical_int_flag_sql("source_rows.source_code is not null and source_rows.source_code_type is null") }} as source_code_type_null_when_source_code_present
        , {{ dq_logical_int_flag_sql("source_rows.source_code_type is not null and not (" ~ source_code_type_valid_where ~ ")") }} as source_code_type_invalid
        , {{ dq_logical_int_flag_sql("source_rows.source_code_type is not null and source_rows.source_code is null") }} as source_code_null
        , {{ dq_logical_int_flag_sql(
              "source_rows.source_code is not null "
              ~ "and ("
              ~ "(lower(cast(source_rows.source_code_type as " ~ string_type ~ ")) = 'ndc' and source_ndc_rows.ndc is null) "
              ~ "or (lower(cast(source_rows.source_code_type as " ~ string_type ~ ")) = 'rxnorm' and source_rxnorm_rows.rxcui is null) "
              ~ "or (lower(cast(source_rows.source_code_type as " ~ string_type ~ ")) = 'atc' and source_atc_rows.atc_code is null)"
              ~ ")"
          ) }} as source_code_invalid
        , {{ dq_logical_int_flag_sql("source_rows.ndc_code is not null and ndc_rows.ndc is null") }} as ndc_code_invalid
        , {{ dq_logical_int_flag_sql("source_rows.rxnorm_code is not null and rxnorm_rows.rxcui is null") }} as rxnorm_code_invalid
        , {{ dq_logical_int_flag_sql("source_rows.atc_code is not null and atc_rows.atc_code is null") }} as atc_code_invalid
        , {{ dq_logical_int_flag_sql("source_rows.quantity is not null and source_rows.quantity < 0") }} as quantity_negative
        , {{ dq_logical_int_flag_sql("source_rows.days_supply is not null and source_rows.days_supply < 0") }} as days_supply_negative
    from source_rows
    left join patient_rows as patient_person
        on source_rows.person_id = patient_person.person_id
       and source_rows.data_source = patient_person.data_source
    left join patient_rows as patient_patient
        on source_rows.patient_id = patient_patient.patient_id
       and source_rows.data_source = patient_patient.data_source
    left join encounter_rows
        on source_rows.encounter_id = encounter_rows.encounter_id
       and source_rows.data_source = encounter_rows.data_source
    left join practitioner_rows
        on source_rows.practitioner_id = practitioner_rows.practitioner_id
       and source_rows.data_source = practitioner_rows.data_source
    left join ndc_rows as source_ndc_rows
        on replace(cast(source_rows.source_code as {{ string_type }}), '-', '') = replace(cast(source_ndc_rows.ndc as {{ string_type }}), '-', '')
       and lower(cast(source_rows.source_code_type as {{ string_type }})) = 'ndc'
    left join rxnorm_rows as source_rxnorm_rows
        on cast(source_rows.source_code as {{ string_type }}) = cast(source_rxnorm_rows.rxcui as {{ string_type }})
       and lower(cast(source_rows.source_code_type as {{ string_type }})) = 'rxnorm'
    left join atc_rows as source_atc_rows
        on cast(source_rows.source_code as {{ string_type }}) = cast(source_atc_rows.atc_code as {{ string_type }})
       and lower(cast(source_rows.source_code_type as {{ string_type }})) = 'atc'
    left join ndc_rows
        on replace(cast(source_rows.ndc_code as {{ string_type }}), '-', '') = replace(cast(ndc_rows.ndc as {{ string_type }}), '-', '')
    left join rxnorm_rows
        on cast(source_rows.rxnorm_code as {{ string_type }}) = cast(rxnorm_rows.rxcui as {{ string_type }})
    left join atc_rows
        on cast(source_rows.atc_code as {{ string_type }}) = cast(atc_rows.atc_code as {{ string_type }})
)

select *
from final
