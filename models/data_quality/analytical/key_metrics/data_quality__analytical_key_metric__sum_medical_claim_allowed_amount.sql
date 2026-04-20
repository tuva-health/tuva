{{ dq_config_analytical_metric_model('analytical_key_metric__sum_medical_claim_allowed_amount') }}

{% set core_medical_claim_rel = dq_analytical_relation('core__medical_claim') %}

{% if execute and core_medical_claim_rel is not none %}
    select
          sources.data_source
        , 'basic claims' as domain
        , 'sum allowed_amount in core.medical_claim' as metric
        , {{ dq_analytical_decimal_result_sql("coalesce(allowed_totals.result, 0)") }} as result
    from (
        {{ dq_source_dimension_sql(core_medical_claim_rel) }}
    ) as sources
    left join (
        select
              coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
            , coalesce(sum(allowed_amount), 0) as result
        from {{ core_medical_claim_rel }}
        group by 1
    ) as allowed_totals
        on sources.data_source_key = allowed_totals.data_source_key
{% else %}
    {{ dq_analytical_empty_result_sql() }}
{% endif %}
