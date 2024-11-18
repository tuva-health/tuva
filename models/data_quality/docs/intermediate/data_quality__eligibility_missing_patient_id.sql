{{ config(
  enabled=false
) }}


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
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from eligibility_spans
where
    patient_id is null or patient_id = ''