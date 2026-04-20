{{ dq_config_analytical_metric_model('analytical_key_metric__count_distinct_medical_claims') }}

{% set core_medical_claim_rel = dq_analytical_relation('core__medical_claim') %}

{% if execute and core_medical_claim_rel is not none %}
    select
          sources.data_source
        , 'basic claims' as domain
        , 'count distinct claim_id in core.medical_claim' as metric
        , {{ dq_analytical_count_result_sql("coalesce(claim_counts.result, 0)") }} as result
    from (
        {{ dq_source_dimension_sql(core_medical_claim_rel) }}
    ) as sources
    left join (
        select
              coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
            , count(distinct claim_id) as result
        from {{ core_medical_claim_rel }}
        group by 1
    ) as claim_counts
        on sources.data_source_key = claim_counts.data_source_key
{% else %}
    {{ dq_analytical_empty_result_sql() }}
{% endif %}
