{{ config(
     enabled = var('enable_data_quality', false) | as_bool,
     schema = dq_data_quality_schema_name(),
     alias = 'analytical_key_metrics',
     tags = ['data_quality', 'dq', 'dq2', 'dq_analytics', 'dq_analytical'],
     materialized = 'table'
   )
}}

{% set metric_manifest = dq_analytical_metric_manifest() %}

{% if metric_manifest | length > 0 %}
    with metric_manifest as (
        {% for spec in metric_manifest %}
            select
                  cast('{{ spec['model_name'] }}' as {{ dbt.type_string() }}) as model_name
                , cast({{ spec['sort_order'] }} as {{ dbt.type_int() }}) as sort_order
                , cast('{{ spec['result_type'] }}' as {{ dbt.type_string() }}) as result_type
            {% if not loop.last %}union all{% endif %}
        {% endfor %}
    ),
    unioned_metrics as (
        {% for spec in metric_manifest %}
            select
                  cast('{{ spec['model_name'] }}' as {{ dbt.type_string() }}) as model_name
                , cast(data_source as {{ dbt.type_string() }}) as data_source
                , cast(domain as {{ dbt.type_string() }}) as domain
                , cast(metric as {{ dbt.type_string() }}) as metric
                , cast(result as {{ dbt.type_numeric() }}) as result
            from {{ ref(spec['model_name']) }}
            {% if not loop.last %}union all{% endif %}
        {% endfor %}
    )
    select
          cast(unioned_metrics.data_source as {{ dbt.type_string() }}) as data_source
        , cast(unioned_metrics.domain as {{ dbt.type_string() }}) as domain
        , cast(unioned_metrics.metric as {{ dbt.type_string() }}) as metric
        , cast(
            case
                when unioned_metrics.result is null then null
                when metric_manifest.result_type = 'count'
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
        , cast(null as {{ dbt.type_string() }}) as medicare
        , cast(null as {{ dbt.type_string() }}) as commercial
        , cast(null as {{ dbt.type_string() }}) as medicaid
    from unioned_metrics
    inner join metric_manifest
        on unioned_metrics.model_name = metric_manifest.model_name
{% else %}
    {{ dq_analytical_empty_summary_result_sql() }}
{% endif %}
