{{ config(
    enabled = var('claims_enabled', False)
) }}

with medical_claim_rows as (

    select count(*) as totalcount
    from {{ ref('medical_claim') }}

)

, medical_claim_distinct_pk as (

    select count(distinct {{ dbt.concat(["claim_id", "'|'", "cast(claim_line_number as " ~ dbt.type_string() ~ ")"]) }}) as countpk
    from {{ ref('medical_claim') }}

)

, pharmacy_claim_rows as (

    select count(*) as totalcount
    from {{ ref('pharmacy_claim') }}

)

, pharmacy_claim_distinct_pk as (

    select count(distinct {{ dbt.concat(["claim_id", "'|'", "cast(claim_line_number as " ~ dbt.type_string() ~ ")"]) }}) as countpk
    from {{ ref('pharmacy_claim') }}

)

, eligibility_rows as (

    select count(*) as totalcount
    from {{ ref('eligibility') }}

)

, eligibility_distinct_pk as (

    select 
        count(distinct {{ dbt.concat([
        "person_id", 
        "'|'", 
        quote_column("plan"),
        "'|'", 
        "cast(enrollment_start_date as " ~ dbt.type_string() ~ ")", 
        "'|'", 
        "cast(enrollment_end_date as " ~ dbt.type_string() ~ ")"
    ]) }}) as countpk
    from {{ ref('eligibility') }}

)

, final as (

    select
        'medical_claim has correct PK' as field,
        case
            when (select * from medical_claim_rows) = (select * from medical_claim_distinct_pk) then 'YES'
            else 'NO'
        end as field_value

    union all

    select
        'pharmacy_claim has correct PK' as field,
        case
            when (select * from pharmacy_claim_rows) = (select * from pharmacy_claim_distinct_pk) then 'YES'
            else 'NO'
        end as field_value

    union all

    select
        'eligibility has correct PK' as field,
        case
            when (select * from eligibility_rows) = (select * from eligibility_distinct_pk) then 'YES'
            else 'NO'
        end as field_value

)

select
      field
    , field_value
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from final
