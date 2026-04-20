{{ dq_config_analytical_metric_model('analytical_key_metric__members_with_claims_missing_enrollment_month') }}

{% set core_medical_claim_rel = dq_analytical_relation('core__medical_claim') %}
{% set core_pharmacy_claim_rel = dq_analytical_relation('core__pharmacy_claim') %}
{% set core_member_months_rel = dq_analytical_relation('core__member_months') %}
{% set claim_month_queries = [] %}

{% if core_medical_claim_rel is not none %}
    {% set medical_claim_months_query %}
        select distinct
              cast(data_source as {{ dbt.type_string() }}) as data_source
            , person_id
            , {{ year_month('claim_start_date') }} as year_month
        from {{ core_medical_claim_rel }}
        where claim_start_date is not null
    {% endset %}
    {% do claim_month_queries.append(medical_claim_months_query) %}
{% endif %}

{% if core_pharmacy_claim_rel is not none %}
    {% set pharmacy_claim_months_query %}
        select distinct
              cast(data_source as {{ dbt.type_string() }}) as data_source
            , person_id
            , {{ year_month('dispensing_date') }} as year_month
        from {{ core_pharmacy_claim_rel }}
        where dispensing_date is not null
    {% endset %}
    {% do claim_month_queries.append(pharmacy_claim_months_query) %}
{% endif %}

{% if execute and claim_month_queries | length > 0 and core_member_months_rel is not none %}
    select
          sources.data_source
        , 'basic enrollment' as domain
        , 'members w/ claims missing enrollment that month' as metric
        , {{ dq_analytical_count_result_sql("coalesce(missing_months.result, 0)") }} as result
    from (
        select distinct
              source_rows.data_source_key
            , source_rows.data_source
        from (
            {% if core_medical_claim_rel is not none and core_pharmacy_claim_rel is not none %}
                {{ dq_source_dimension_sql(core_medical_claim_rel) }}
                union all
                {{ dq_source_dimension_sql(core_pharmacy_claim_rel) }}
            {% elif core_medical_claim_rel is not none %}
                {{ dq_source_dimension_sql(core_medical_claim_rel) }}
            {% else %}
                {{ dq_source_dimension_sql(core_pharmacy_claim_rel) }}
            {% endif %}
        ) as source_rows
    ) as sources
    left join (
        select
              claim_member_months.data_source_key
            , count(distinct claim_member_months.person_id) as result
        from (
            select
                  coalesce(data_source, '{{ dq_source_key_sentinel() }}') as data_source_key
                , data_source
                , person_id
                , year_month
            from (
                {{ claim_month_queries | join('\nunion\n') }}
            ) as claim_member_months
        ) as claim_member_months
        left join (
            select distinct
                  coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                , person_id
                , year_month
            from {{ core_member_months_rel }}
        ) as member_months
            on claim_member_months.data_source_key = member_months.data_source_key
            and claim_member_months.person_id = member_months.person_id
            and claim_member_months.year_month = member_months.year_month
        where member_months.person_id is null
        group by 1
    ) as missing_months
        on sources.data_source_key = missing_months.data_source_key
{% else %}
    {{ dq_analytical_empty_result_sql() }}
{% endif %}
