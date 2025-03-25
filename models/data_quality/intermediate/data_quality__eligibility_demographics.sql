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
        , birth_date
        , gender
    from {{ ref('input_layer__eligibility') }}
)

, missing_birth_date as(
    select
    'Missing birthday' as data_quality_check
    , count(distinct eligibility_span_id) as result_count
    from eligibility_spans
    where birth_date is null
)
, invalid_birth_date as(
    select
    'Birthday is not a valid date' as data_quality_check
    , count(distinct eligibility_span_id) as result_count
    from eligibility_spans e
    left join {{ ref('reference_data__calendar') }} c
        on e.birth_date = c.full_date
    where c.full_date is null
)
, future_birth_date as(
    select
    'Birthday is in the future' as data_quality_check
    , count(distinct eligibility_span_id) as result_count
    from eligibility_spans
    {% if target.type == 'fabric' %}
        where birth_date > GETDATE()
    {% else %}
        where birth_date > current_date
    {% endif %}
)
, past_birth_date as(
    select
    'Birthday is too far in the past' as data_quality_check
    , count(distinct eligibility_span_id) as result_count
    from eligibility_spans
    {% if target.type == 'fabric' %}
        where cast(floor({{ datediff('birth_date', 'getdate()', 'hour') }} / 8760.0) as {{ dbt.type_int() }} ) > 110
    {% else %}
        where cast(floor({{ datediff('birth_date', 'current_date', 'hour') }} / 8760.0) as {{ dbt.type_int() }} ) > 110
    {% endif %}

)
, multiple_birth_date as(
    select
    'Patient has multiple birthdays' as data_quality_check
    , count(distinct person_id) as result_count
    from(
        select
            person_id
            , birth_date
            , rank() over (partition by person_id, birth_date order by birth_date) as rank_birth_date
        from eligibility_spans e
        where birth_date is not null
    )x
where rank_birth_date > 1
)
, missing_gender as(
    select
    'Missing gender' as data_quality_check
    , count(distinct eligibility_span_id) as result_count
    from eligibility_spans
    where birth_date is null
)
, invalid_gender as(
    select
    'Patient gender does not join to terminology table' as data_quality_check
    , count(distinct eligibility_span_id) as result_count
    from eligibility_spans
    where birth_date is null
)

select *, '{{ var('tuva_last_run')}}' as tuva_last_run from missing_birth_date
union all
select *, '{{ var('tuva_last_run')}}' as tuva_last_run from invalid_birth_date
union all
select *, '{{ var('tuva_last_run')}}' as tuva_last_run from future_birth_date
union all
select *, '{{ var('tuva_last_run')}}' as tuva_last_run from past_birth_date
union all
select *, '{{ var('tuva_last_run')}}' as tuva_last_run from multiple_birth_date
union all
select *, '{{ var('tuva_last_run')}}' as tuva_last_run from missing_gender
union all
select *, '{{ var('tuva_last_run')}}' as tuva_last_run from invalid_gender
