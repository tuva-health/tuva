{{ dq_config_analytical_metric_model('analytical_key_metric__sum_medical_claim_allowed_amount_by_claim_type') }}

{% set core_medical_claim_rel = dq_analytical_relation('core__medical_claim') %}

{% if execute and core_medical_claim_rel is not none %}
    select
          cast(data_source as {{ dbt.type_string() }}) as data_source
        , 'basic claims' as domain
        , {{ concat_custom([
            "'sum allowed_amount by claim_type | '",
            "coalesce(cast(claim_type as " ~ dbt.type_string() ~ "), cast('unknown' as " ~ dbt.type_string() ~ "))"
          ]) }} as metric
        , {{ dq_analytical_decimal_result_sql("coalesce(sum(allowed_amount), 0)") }} as result
    from {{ core_medical_claim_rel }}
    group by 1, 3
{% else %}
    {{ dq_analytical_empty_result_sql() }}
{% endif %}
