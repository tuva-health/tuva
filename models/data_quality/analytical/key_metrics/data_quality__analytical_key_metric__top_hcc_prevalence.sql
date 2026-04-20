{{ dq_config_analytical_metric_model('analytical_key_metric__top_hcc_prevalence') }}

{% set cms_hcc_rel = dq_analytical_relation('cms_hcc__patient_risk_factors') %}
{% set core_patient_rel = dq_analytical_relation('core__patient') %}
{% set cms_hcc_columns = dq_actual_columns(cms_hcc_rel) if cms_hcc_rel is not none else [] %}
{% set patient_columns = dq_actual_columns(core_patient_rel) if core_patient_rel is not none else [] %}
{% set join_on_data_source = dq_has_column(cms_hcc_columns, 'data_source') and dq_has_column(patient_columns, 'data_source') %}

{% if execute and cms_hcc_rel is not none and core_patient_rel is not none %}
    select
          ranked_hcc.data_source
        , 'top hcc prevalence' as domain
        , {{ concat_custom([
            "'prevalence | '",
            "ranked_hcc.risk_factor_description"
          ]) }} as metric
        , {{ dq_analytical_decimal_result_sql(
            "case when patient_totals.total_patients = 0 then null else (cast(ranked_hcc.patient_count as " ~ dbt.type_numeric() ~ ") / cast(patient_totals.total_patients as " ~ dbt.type_numeric() ~ ")) * 100 end"
          ) }} as result
    from (
        select
              hcc_counts.data_source
            , hcc_counts.risk_factor_description
            , hcc_counts.patient_count
            , row_number() over (
                partition by hcc_counts.data_source
                order by hcc_counts.patient_count desc, hcc_counts.risk_factor_description
              ) as hcc_rank
        from (
            select
                  cast(patient.data_source as {{ dbt.type_string() }}) as data_source
                , cast(hcc.risk_factor_description as {{ dbt.type_string() }}) as risk_factor_description
                , count(distinct hcc.person_id) as patient_count
            from {{ cms_hcc_rel }} as hcc
            inner join {{ core_patient_rel }} as patient
                on hcc.person_id = patient.person_id
               {% if join_on_data_source %}
               and coalesce(cast(hcc.data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}')
                = coalesce(cast(patient.data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}')
               {% endif %}
            where hcc.factor_type = 'Disease'
            group by 1, 2
        ) as hcc_counts
    ) as ranked_hcc
    inner join (
        select
              cast(data_source as {{ dbt.type_string() }}) as data_source
            , count(distinct person_id) as total_patients
        from {{ core_patient_rel }}
        group by 1
    ) as patient_totals
        on ranked_hcc.data_source = patient_totals.data_source
    where ranked_hcc.hcc_rank <= 10
{% else %}
    {{ dq_analytical_empty_result_sql() }}
{% endif %}
