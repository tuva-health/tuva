{{ dq_config_analytical_metric_model('analytical_key_metric__total_member_months') }}

{% set core_member_months_rel = dq_analytical_relation('core__member_months') %}

{% if execute and core_member_months_rel is not none %}
    select
          sources.data_source
        , 'basic enrollment' as domain
        , 'total member months' as metric
        , {{ dq_analytical_count_result_sql("coalesce(total_member_months.result, 0)") }} as result
    from (
        {{ dq_source_dimension_sql(core_member_months_rel) }}
    ) as sources
    left join (
        select
              coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
            , count(*) as result
        from {{ core_member_months_rel }}
        group by 1
    ) as total_member_months
        on sources.data_source_key = total_member_months.data_source_key
{% else %}
    {{ dq_analytical_empty_result_sql() }}
{% endif %}
