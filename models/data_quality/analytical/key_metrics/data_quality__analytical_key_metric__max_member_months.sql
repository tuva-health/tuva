{{ dq_config_analytical_metric_model('analytical_key_metric__max_member_months') }}

{% set core_member_months_rel = dq_analytical_relation('core__member_months') %}

{% if execute and core_member_months_rel is not none %}
    select
          sources.data_source
        , 'basic enrollment' as domain
        , 'max member months' as metric
        , {{ dq_analytical_count_result_sql("coalesce(member_month_maxima.result, 0)") }} as result
    from (
        {{ dq_source_dimension_sql(core_member_months_rel) }}
    ) as sources
    left join (
        select
              member_month_counts.data_source_key
            , max(member_month_counts.member_months) as result
        from (
            select
                  coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                , person_id
                , count(*) as member_months
            from {{ core_member_months_rel }}
            group by 1, 2
        ) as member_month_counts
        group by 1
    ) as member_month_maxima
        on sources.data_source_key = member_month_maxima.data_source_key
{% else %}
    {{ dq_analytical_empty_result_sql() }}
{% endif %}
