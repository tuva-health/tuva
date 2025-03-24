{{ config(
     enabled = (var('enable_legacy_data_quality', False) and var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
)}}


with eligibility_spans as(
    select distinct
        {{ concat_custom([
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
        , person_id
    from {{ ref('input_layer__eligibility') }}
)

select
    'Missing person_id' as data_quality_check
    ,count(*) as result_count
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from eligibility_spans
where
    person_id is null or person_id = ''