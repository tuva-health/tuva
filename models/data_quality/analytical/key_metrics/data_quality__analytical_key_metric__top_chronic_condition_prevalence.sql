{{ dq_config_analytical_metric_model('analytical_key_metric__top_chronic_condition_prevalence') }}

{% set chronic_conditions_rel = dq_analytical_relation('chronic_conditions__tuva_chronic_conditions_long') %}
{% set core_patient_rel = dq_analytical_relation('core__patient') %}
{% set chronic_condition_columns = dq_actual_columns(chronic_conditions_rel) if chronic_conditions_rel is not none else [] %}
{% set patient_columns = dq_actual_columns(core_patient_rel) if core_patient_rel is not none else [] %}
{% set join_on_data_source = dq_has_column(chronic_condition_columns, 'data_source') and dq_has_column(patient_columns, 'data_source') %}

{% if execute and chronic_conditions_rel is not none and core_patient_rel is not none %}
    select
          ranked_conditions.data_source
        , 'top chronic condition prevalence' as domain
        , {{ concat_custom([
            "'prevalence | '",
            "ranked_conditions.condition"
          ]) }} as metric
        , {{ dq_analytical_decimal_result_sql(
            "case when patient_totals.total_patients = 0 then null else (cast(ranked_conditions.patient_count as " ~ dbt.type_numeric() ~ ") / cast(patient_totals.total_patients as " ~ dbt.type_numeric() ~ ")) * 100 end"
          ) }} as result
    from (
        select
              condition_counts.data_source
            , condition_counts.condition
            , condition_counts.patient_count
            , row_number() over (
                partition by condition_counts.data_source
                order by condition_counts.patient_count desc, condition_counts.condition
              ) as condition_rank
        from (
            select
                  cast(patient.data_source as {{ dbt.type_string() }}) as data_source
                , cast(chronic_conditions.condition as {{ dbt.type_string() }}) as condition
                , count(distinct chronic_conditions.person_id) as patient_count
            from {{ chronic_conditions_rel }} as chronic_conditions
            inner join {{ core_patient_rel }} as patient
                on chronic_conditions.person_id = patient.person_id
               {% if join_on_data_source %}
               and coalesce(cast(chronic_conditions.data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}')
                = coalesce(cast(patient.data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}')
               {% endif %}
            group by 1, 2
        ) as condition_counts
    ) as ranked_conditions
    inner join (
        select
              cast(data_source as {{ dbt.type_string() }}) as data_source
            , count(distinct person_id) as total_patients
        from {{ core_patient_rel }}
        group by 1
    ) as patient_totals
        on ranked_conditions.data_source = patient_totals.data_source
    where ranked_conditions.condition_rank <= 10
{% else %}
    {{ dq_analytical_empty_result_sql() }}
{% endif %}
