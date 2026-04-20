{{ dq_config_analytical_metric_model('analytical_key_metric__members_with_claims_without_enrollment') }}

{% set core_medical_claim_rel = dq_analytical_relation('core__medical_claim') %}
{% set core_pharmacy_claim_rel = dq_analytical_relation('core__pharmacy_claim') %}
{% set core_eligibility_rel = dq_analytical_relation('core__eligibility') %}
{% set claims_member_queries = [] %}

{% if core_medical_claim_rel is not none %}
    {% set medical_claim_members_query %}
        select distinct
              cast(data_source as {{ dbt.type_string() }}) as data_source
            , person_id
        from {{ core_medical_claim_rel }}
    {% endset %}
    {% do claims_member_queries.append(medical_claim_members_query) %}
{% endif %}

{% if core_pharmacy_claim_rel is not none %}
    {% set pharmacy_claim_members_query %}
        select distinct
              cast(data_source as {{ dbt.type_string() }}) as data_source
            , person_id
        from {{ core_pharmacy_claim_rel }}
    {% endset %}
    {% do claims_member_queries.append(pharmacy_claim_members_query) %}
{% endif %}

{% if execute and claims_member_queries | length > 0 and core_eligibility_rel is not none %}
    select
          sources.data_source
        , 'basic enrollment' as domain
        , 'members w/ claims w/o any enrollment' as metric
        , {{ dq_analytical_count_result_sql("coalesce(missing_members.result, 0)") }} as result
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
              claim_members.data_source_key
            , count(distinct claim_members.person_id) as result
        from (
            select
                  coalesce(data_source, '{{ dq_source_key_sentinel() }}') as data_source_key
                , data_source
                , person_id
            from (
                {{ claims_member_queries | join('\nunion\n') }}
            ) as claim_members
        ) as claim_members
        left join (
            select distinct
                  coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                , person_id
            from {{ core_eligibility_rel }}
        ) as eligible_members
            on claim_members.data_source_key = eligible_members.data_source_key
            and claim_members.person_id = eligible_members.person_id
        where eligible_members.person_id is null
        group by 1
    ) as missing_members
        on sources.data_source_key = missing_members.data_source_key
{% else %}
    {{ dq_analytical_empty_result_sql() }}
{% endif %}
