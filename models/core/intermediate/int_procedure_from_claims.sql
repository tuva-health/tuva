{{ config(
     enabled = var('claims_enabled', False) | as_bool
   )
}}

with encounter_crosswalk as (
    select
        claim_id
        , claim_line_number
        , encounter_id
    from {{ ref('encounters__combined_claim_line_crosswalk') }}
    where claim_line_attribution_number = 1
)

, distinct_claim_encounters as (
    select distinct
        claim_id
        , encounter_id
    from encounter_crosswalk
)

, hcpcs_procedures as (
    select
        procedure_source.person_id
        , procedure_source.member_id
        , procedure_source.claim_id
        , procedure_source.claim_line_number
        , procedure_source.procedure_sequence_id
        , enc.encounter_id
        , procedure_source.procedure_date
        , procedure_source.practitioner_id
        , procedure_source.code_system
        , procedure_source.source_code
        , procedure_source.source_description
        , procedure_source.normalized_code
        , procedure_source.normalized_description
        , procedure_source.modifier_1
        , procedure_source.modifier_2
        , procedure_source.modifier_3
        , procedure_source.modifier_4
        , procedure_source.modifier_5
        , procedure_source.tuva_last_run
        , procedure_source.data_source
    from {{ ref('normalized__medical_claim_procedures') }} as procedure_source
    left join encounter_crosswalk as enc
        on procedure_source.claim_id = enc.claim_id
        and procedure_source.claim_line_number = enc.claim_line_number
    where procedure_source.claim_line_number is not null
)

, claim_header_procedures as (
    select
        procedure_source.person_id
        , procedure_source.member_id
        , procedure_source.claim_id
        , procedure_source.claim_line_number
        , procedure_source.procedure_sequence_id
        , enc.encounter_id
        , procedure_source.procedure_date
        , procedure_source.practitioner_id
        , procedure_source.code_system
        , procedure_source.source_code
        , procedure_source.source_description
        , procedure_source.normalized_code
        , procedure_source.normalized_description
        , procedure_source.modifier_1
        , procedure_source.modifier_2
        , procedure_source.modifier_3
        , procedure_source.modifier_4
        , procedure_source.modifier_5
        , procedure_source.tuva_last_run
        , procedure_source.data_source
    from {{ ref('normalized__medical_claim_procedures') }} as procedure_source
    left join distinct_claim_encounters as enc
        on procedure_source.claim_id = enc.claim_id
    where procedure_source.claim_line_number is null
)

, all_claim_procedures as (
    select * from hcpcs_procedures
    union all
    select * from claim_header_procedures
)

select distinct
    {{ dbt.safe_cast(
        concat_custom([
            "'claims'",
            "'_'",
            "cast(data_source as " ~ dbt.type_string() ~ ")",
            "'_'",
            "cast(claim_id as " ~ dbt.type_string() ~ ")",
            "'_'",
            "coalesce(cast(encounter_id as " ~ dbt.type_string() ~ "), 'no_encounter')",
            "'_'",
            "cast(procedure_sequence_id as " ~ dbt.type_string() ~ ")",
            "'_'",
            "cast(coalesce(code_system, 'unknown') as " ~ dbt.type_string() ~ ")",
            "'_'",
            "cast(source_code as " ~ dbt.type_string() ~ ")",
            "case when procedure_date is not null then concat('_', cast(procedure_date as " ~ dbt.type_string() ~ ")) else '' end",
            "case when modifier_1 is not null then concat('_', modifier_1) else '' end",
            "case when modifier_2 is not null then concat('_', modifier_2) else '' end",
            "case when modifier_3 is not null then concat('_', modifier_3) else '' end",
            "case when modifier_4 is not null then concat('_', modifier_4) else '' end",
            "case when modifier_5 is not null then concat('_', modifier_5) else '' end",
            "case when practitioner_id is not null then concat('_', practitioner_id) else '' end"
        ]), api.Column.translate_type("string"))
    }} as procedure_id
    , cast(person_id as {{ dbt.type_string() }}) as person_id
    , cast(member_id as {{ dbt.type_string() }}) as member_id
    , cast(null as {{ dbt.type_string() }}) as patient_id
    , cast(encounter_id as {{ dbt.type_string() }}) as encounter_id
    , cast(claim_id as {{ dbt.type_string() }}) as claim_id
    , {{ try_to_cast_date('procedure_date', 'YYYY-MM-DD') }} as procedure_date
    , cast(practitioner_id as {{ dbt.type_string() }}) as practitioner_id
    , cast(code_system as {{ dbt.type_string() }}) as code_system
    , cast(source_code as {{ dbt.type_string() }}) as source_code
    , cast(source_description as {{ dbt.type_string() }}) as source_description
    , cast(normalized_code as {{ dbt.type_string() }}) as normalized_code
    , cast(normalized_description as {{ dbt.type_string() }}) as normalized_description
    , cast(modifier_1 as {{ dbt.type_string() }}) as modifier_1
    , cast(modifier_2 as {{ dbt.type_string() }}) as modifier_2
    , cast(modifier_3 as {{ dbt.type_string() }}) as modifier_3
    , cast(modifier_4 as {{ dbt.type_string() }}) as modifier_4
    , cast(modifier_5 as {{ dbt.type_string() }}) as modifier_5
    , cast(tuva_last_run as {{ dbt.type_timestamp() }}) as tuva_last_run
    , cast(data_source as {{ dbt.type_string() }}) as data_source
from all_claim_procedures
