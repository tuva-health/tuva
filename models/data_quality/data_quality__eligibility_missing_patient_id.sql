{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}


with eligibility_spans as(
    select distinct
        {{ dbt.concat([
            "member_id",
            "'-'",
            "enrollment_start_date",
            "'-'",
            "enrollment_end_date",
            "'-'",
            "payer",
            "'-'",
            quote_column('plan'),
        ]) }} as eligibility_span_id
        , patient_id
    from {{ ref('eligibility') }}
)

select
    'Missing patient_id' as data_quality_check
    ,count(*) as result_count
from eligibility_spans
where
    patient_id is null or patient_id = ''