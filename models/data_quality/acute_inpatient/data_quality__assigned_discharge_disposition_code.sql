{{ config(
    enabled = var('claims_enabled', False)
) }}

with unique_codes as (

    select
          claim_id
        , discharge_disposition_code
        , 'unique' as assignment_method
    from {{ ref('data_quality__unique_discharge_disposition_code') }}

)

, determinable_codes as (

    select
          claim_id
        , discharge_disposition_code_1 as discharge_disposition_code
        , 'determinable' as assignment_method
    from {{ ref('data_quality__determinable_discharge_disposition_code') }}

)

, undeterminable_codes as (

    select
          claim_id
        , cast(null as {{ dbt.type_string() }}) as discharge_disposition_code
        , 'undeterminable' as assignment_method
    from {{ ref('data_quality__undeterminable_discharge_disposition_code') }}

)

, union_of_codes as (

    select
          claim_id
        , discharge_disposition_code
        , assignment_method
    from unique_codes

    union all

    select
          claim_id
        , discharge_disposition_code
        , assignment_method
    from determinable_codes

    union all

    select
          claim_id
        , discharge_disposition_code
        , assignment_method
    from undeterminable_codes

)

select
      claim_id
    , discharge_disposition_code
    , assignment_method
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from union_of_codes
