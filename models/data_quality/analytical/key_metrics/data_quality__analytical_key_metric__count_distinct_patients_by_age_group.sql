{{ dq_config_analytical_metric_model('analytical_key_metric__count_distinct_patients_by_age_group') }}

{% set core_patient_rel = dq_analytical_relation('core__patient') %}

{% if execute and core_patient_rel is not none %}
    select
          cast(data_source as {{ dbt.type_string() }}) as data_source
        , 'patient demographics' as domain
        , {{ concat_custom([
            "'count distinct patients | age_group = '",
            "coalesce(cast(age_group as " ~ dbt.type_string() ~ "), cast('unknown' as " ~ dbt.type_string() ~ "))"
          ]) }} as metric
        , {{ dq_analytical_count_result_sql("count(distinct person_id)") }} as result
    from {{ core_patient_rel }}
    group by 1, 3
{% else %}
    {{ dq_analytical_empty_result_sql() }}
{% endif %}
