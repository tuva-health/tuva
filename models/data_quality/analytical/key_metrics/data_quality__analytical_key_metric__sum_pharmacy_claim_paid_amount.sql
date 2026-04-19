{{ dq_config_analytical_metric_model('analytical_key_metric__sum_pharmacy_claim_paid_amount') }}

{% set core_pharmacy_claim_rel = dq_analytical_relation('core__pharmacy_claim') %}

{% if execute and core_pharmacy_claim_rel is not none %}
    select
          sources.data_source
        , 'basic claims' as domain
        , 'sum paid_amount in core.pharmacy_claim' as metric
        , {{ dq_analytical_decimal_result_sql("coalesce(paid_totals.result, 0)") }} as result
    from (
        {{ dq_source_dimension_sql(core_pharmacy_claim_rel) }}
    ) as sources
    left join (
        select
              coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
            , coalesce(sum(paid_amount), 0) as result
        from {{ core_pharmacy_claim_rel }}
        group by 1
    ) as paid_totals
        on sources.data_source_key = paid_totals.data_source_key
{% else %}
    {{ dq_analytical_empty_result_sql() }}
{% endif %}
