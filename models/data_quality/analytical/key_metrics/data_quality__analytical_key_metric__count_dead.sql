{{ dq_config_analytical_metric_model('analytical_key_metric__count_dead') }}

{% set core_patient_rel = dq_analytical_relation('core__patient') %}

{% if execute and core_patient_rel is not none %}
    select
          sources.data_source
        , 'patient demographics' as domain
        , 'count dead' as metric
        , {{ dq_analytical_count_result_sql("coalesce(dead_counts.result, 0)") }} as result
    from (
        {{ dq_source_dimension_sql(core_patient_rel) }}
    ) as sources
    left join (
        select
              coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
            , sum(
                case
                    when coalesce(death_flag, 0) = 1 or death_date is not null then 1
                    else 0
                end
              ) as result
        from {{ core_patient_rel }}
        group by 1
    ) as dead_counts
        on sources.data_source_key = dead_counts.data_source_key
{% else %}
    {{ dq_analytical_empty_result_sql() }}
{% endif %}
