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
        , payer
        , payer_type
    from {{ ref('input_layer__eligibility') }}
)

, missing_payer_type as(
    select
        'Missing payer type' as data_quality_check
        , count(distinct eligibility_span_id) as result_count
    from eligibility_spans
    where payer_type is null
)
, missing_payer_name as(
    select
        'Missing payer name' as data_quality_check
        , count(distinct eligibility_span_id) as result_count
    from eligibility_spans
    where payer is null
)
, invalid_payer_type as(
    select
        'Payer type does not join to terminology table' as data_quality_check
        , count(distinct eligibility_span_id) as result_count
    from eligibility_spans e
    left join {{ ref('terminology__payer_type') }} pt
        on e.payer_type = pt.payer_type
    where pt.payer_type is null
)

select * , '{{ var('tuva_last_run')}}' as tuva_last_run from missing_payer_type
union all
select * , '{{ var('tuva_last_run')}}' as tuva_last_run from missing_payer_name
union all
select * , '{{ var('tuva_last_run')}}' as tuva_last_run from invalid_payer_type