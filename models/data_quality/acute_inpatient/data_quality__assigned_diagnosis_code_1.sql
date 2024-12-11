{{ config(
    enabled = var('claims_enabled', False)
) }}

with unique_codes as (
    select
          claim_id
        , diagnosis_code_1
        , 'unique' as assignment_method
    from {{ ref('data_quality__unique_diagnosis_code_1') }}
)

, determinable_codes as (
    select
          claim_id
        , diagnosis_code_1_1 as diagnosis_code_1
        , 'determinable' as assignment_method
    from {{ ref('data_quality__determinable_diagnosis_code_1') }}
)

, undeterminable_codes as (
    select
          claim_id
        , cast(null as {{ dbt.type_string() }}) as diagnosis_code_1
        , 'undeterminable' as assignment_method
    from {{ ref('data_quality__undeterminable_diagnosis_code_1') }}
)

, union_of_codes as (
    select
          claim_id
        , diagnosis_code_1
        , assignment_method
    from unique_codes

    union all

    select
          claim_id
        , diagnosis_code_1
        , assignment_method
    from determinable_codes

    union all

    select
          claim_id
        , diagnosis_code_1
        , assignment_method
    from undeterminable_codes
)

select
      claim_id
    , diagnosis_code_1
    , assignment_method
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from union_of_codes
