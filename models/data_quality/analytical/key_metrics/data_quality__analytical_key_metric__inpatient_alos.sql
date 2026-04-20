{{ dq_config_analytical_metric_model('analytical_key_metric__inpatient_alos') }}

{% set core_encounter_rel = dq_analytical_relation('core__encounter') %}

{% if execute and core_encounter_rel is not none %}
    select
          sources.data_source
        , 'acute inpatient' as domain
        , 'inpatient alos' as metric
        , {{ dq_analytical_decimal_result_sql("coalesce(alos.result, 0)") }} as result
    from (
        {{ dq_source_dimension_sql(core_encounter_rel) }}
    ) as sources
    left join (
        select
              coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
            , avg(cast(length_of_stay as {{ dbt.type_numeric() }})) as result
        from {{ core_encounter_rel }}
        where encounter_type = 'acute inpatient'
        group by 1
    ) as alos
        on sources.data_source_key = alos.data_source_key
{% else %}
    {{ dq_analytical_empty_result_sql() }}
{% endif %}
