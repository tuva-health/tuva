{{ config(
    enabled = var('claims_enabled', False)
) }}

with medical_claim_rows as (

    select count(*) as totalcount
    from {{ ref('medical_claim') }}

)

, medical_claim_distinct_pk as (

    select 
        count(distinct {{ dbt.concat(["claim_id", "'|'", "cast(claim_line_number as " ~ dbt.type_string() ~ ")"]) }}) as countpk
    from {{ ref('medical_claim') }}

)

, pharmacy_claim_rows as (

    select count(*) as totalcount
    from {{ ref('pharmacy_claim') }}

)

, pharmacy_claim_distinct_pk as (

    select
        count(distinct {{ dbt.concat(["claim_id", "'|'", "cast(claim_line_number as " ~ dbt.type_string() ~ ")"]) }}) as countpk
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
        'select count(*) from medical_claim' as field,
        (select * from medical_claim_rows) as field_value

    union all

    select
        'select count(distinct claim_id, claim_line_number) from medical_claim' as field,
        (select * from medical_claim_distinct_pk) as field_value

    union all

    select
        'select count(*) from pharmacy_claim' as field,
        (select * from pharmacy_claim_rows) as field_value

    union all

    select
        'select count(distinct claim_id, claim_line_number) from pharmacy_claim' as field,
        (select * from pharmacy_claim_distinct_pk) as field_value

    union all

    select
        'select count(*) from eligibility' as field,
        (select * from eligibility_rows) as field_value

    union all

    select
        'select count(distinct person_id, plan, enrollment_start_date, enrollment_end_date) from eligibility' as field,
        (select * from eligibility_distinct_pk) as field_value

)

select
      field
    , field_value
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from final
