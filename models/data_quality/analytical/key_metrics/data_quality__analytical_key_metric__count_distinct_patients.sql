{{ dq_config_analytical_metric_model('analytical_key_metric__count_distinct_patients') }}

{% set core_patient_rel = dq_analytical_relation('core__patient') %}

{% if execute and core_patient_rel is not none %}
    select
          sources.data_source
        , 'patient demographics' as domain
        , 'count distinct patients' as metric
        , {{ dq_analytical_count_result_sql("coalesce(patient_counts.result, 0)") }} as result
    from (
        {{ dq_source_dimension_sql(core_patient_rel) }}
    ) as sources
    left join (
        select
              coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
            , count(distinct person_id) as result
        from {{ core_patient_rel }}
        group by 1
    ) as patient_counts
        on sources.data_source_key = patient_counts.data_source_key
{% else %}
    {{ dq_analytical_empty_result_sql() }}
{% endif %}
