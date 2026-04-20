{{ config(
     schema = dq_data_quality_schema_name(),
     alias = 'analytical_key_metrics',
     tags = ['data_quality', 'dqi', 'dq2', 'dq_analytical'],
     materialized = 'table'
   )
}}

{% set metric_model_names = dq_analytical_key_metric_model_names() %}

{% if metric_model_names | length > 0 %}
    select
          cast(unioned_metrics.data_source as {{ dbt.type_string() }}) as data_source
        , cast(unioned_metrics.domain as {{ dbt.type_string() }}) as domain
        , cast(unioned_metrics.metric as {{ dbt.type_string() }}) as metric
        , cast(
            case
                when unioned_metrics.result is null then null
                when {{ dq_analytical_metric_is_count_sql('unioned_metrics.metric') }}
                    then cast(
                        {{ dq_analytical_count_result_sql('unioned_metrics.result') }}
                        as {{ dbt.type_string() }}
                    )
                else rtrim(
                    rtrim(
                        cast(
                            {{ dq_analytical_decimal_result_sql('unioned_metrics.result') }}
                            as {{ dbt.type_string() }}
                        ),
                        '0'
                    ),
                    '.'
                )
            end as {{ dbt.type_string() }}
          ) as result
    from (
        {% for metric_model_name in metric_model_names %}
            select
                  cast(data_source as {{ dbt.type_string() }}) as data_source
                , cast(domain as {{ dbt.type_string() }}) as domain
                , cast(metric as {{ dbt.type_string() }}) as metric
                , cast(result as {{ dbt.type_numeric() }}) as result
            from {{ ref(metric_model_name) }}
            {% if not loop.last %}union all{% endif %}
        {% endfor %}
    ) as unioned_metrics
    order by 1, 2, 3
{% else %}
    {{ dq_analytical_empty_result_sql() }}
{% endif %}
