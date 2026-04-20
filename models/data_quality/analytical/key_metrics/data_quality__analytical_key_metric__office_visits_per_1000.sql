{{ dq_config_analytical_metric_model('analytical_key_metric__office_visits_per_1000') }}

{% set core_encounter_rel = dq_analytical_relation('core__encounter') %}
{% set core_member_months_rel = dq_analytical_relation('core__member_months') %}

{% if execute and core_encounter_rel is not none and core_member_months_rel is not none %}
    select
          sources.data_source
        , 'office visits' as domain
        , 'office visits per 1,000' as metric
        , {{ dq_analytical_decimal_result_sql(
            "case when coalesce(member_month_totals.member_months, 0) = 0 then 0 else (cast(coalesce(office_counts.encounter_count, 0) as " ~ dbt.type_numeric() ~ ") / cast(member_month_totals.member_months as " ~ dbt.type_numeric() ~ ")) * 12000 end"
          ) }} as result
    from (
        {{ dq_source_dimension_sql(core_member_months_rel) }}
    ) as sources
    left join (
        select
              coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
            , count(*) as encounter_count
        from {{ core_encounter_rel }}
        where encounter_group = 'office based'
        group by 1
    ) as office_counts
        on sources.data_source_key = office_counts.data_source_key
    left join (
        select
              coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
            , count(*) as member_months
        from {{ core_member_months_rel }}
        group by 1
    ) as member_month_totals
        on sources.data_source_key = member_month_totals.data_source_key
{% else %}
    {{ dq_analytical_empty_result_sql() }}
{% endif %}
