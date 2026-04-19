{{ dq_config_analytical_metric_model('analytical_key_metric__readmission_rate_30_day') }}

{% set readmission_summary_rel = dq_analytical_relation('readmissions__readmission_summary') %}
{% set readmission_augmented_rel = dq_analytical_relation('readmissions__encounter_augmented') %}

{% if execute and readmission_summary_rel is not none and readmission_augmented_rel is not none %}
    select
          sources.data_source
        , 'readmissions' as domain
        , '30-day readmission rate' as metric
        , {{ dq_analytical_decimal_result_sql(
            "case when coalesce(readmission_counts.index_admissions, 0) = 0 then 0 else (cast(readmission_counts.readmissions as " ~ dbt.type_numeric() ~ ") / cast(readmission_counts.index_admissions as " ~ dbt.type_numeric() ~ ")) * 100 end"
          ) }} as result
    from (
        {{ dq_source_dimension_sql(readmission_augmented_rel) }}
    ) as sources
    left join (
        select
              coalesce(cast(augmented.data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
            , sum(case when summary.index_admission_flag = 1 then 1 else 0 end) as index_admissions
            , sum(case when summary.index_admission_flag = 1 and summary.unplanned_readmit_30_flag = 1 then 1 else 0 end) as readmissions
        from {{ readmission_summary_rel }} as summary
        inner join {{ readmission_augmented_rel }} as augmented
            on summary.encounter_id = augmented.encounter_id
        group by 1
    ) as readmission_counts
        on sources.data_source_key = readmission_counts.data_source_key
{% else %}
    {{ dq_analytical_empty_result_sql() }}
{% endif %}
