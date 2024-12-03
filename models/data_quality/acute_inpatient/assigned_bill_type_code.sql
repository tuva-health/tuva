{{ config(
    enabled = var('claims_enabled', False)
) }}

with unique_codes as (

    select
          claim_id
        , bill_type_code
        , 'unique' as assignment_method
    from {{ ref('unique_bill_type_code') }}

)

, determinable_codes as (

    select
          claim_id
        , bill_type_code_1 as bill_type_code
        , 'determinable' as assignment_method
    from {{ ref('determinable_bill_type_code') }}

)

, undeterminable_codes as (

    select
          claim_id
        , cast(null as {{ dbt.type_string() }}) as bill_type_code
        , 'undeterminable' as assignment_method
    from {{ ref('undeterminable_bill_type_code') }}

)

, union_of_codes as (

    select
          claim_id
        , bill_type_code
        , assignment_method
    from unique_codes

    union all

    select
          claim_id
        , bill_type_code
        , assignment_method
    from determinable_codes

    union all

    select
          claim_id
        , bill_type_code
        , assignment_method
    from undeterminable_codes

)

select
      claim_id
    , bill_type_code
    , assignment_method
from union_of_codes
