{% macro dq_analytical_metric_select_sql(source_relation, domain, metric, metric_sql, result_expression='metric_results.result') %}
    select
          cast(sources.data_source as {{ dbt.type_string() }}) as data_source
        , cast({{ dq_analytical_string_literal(domain) }} as {{ dbt.type_string() }}) as domain
        , cast({{ dq_analytical_string_literal(metric) }} as {{ dbt.type_string() }}) as metric
        , cast({{ result_expression }} as {{ dbt.type_numeric() }}) as result
    from (
        {{ dq_source_dimension_sql(source_relation) }}
    ) as sources
    left join (
        {{ metric_sql }}
    ) as metric_results
        on sources.data_source_key = metric_results.data_source_key
{% endmacro %}

{% macro dq_analytical_metric_null_rows_sql(source_relation, domain, metric) %}
    select
          cast(sources.data_source as {{ dbt.type_string() }}) as data_source
        , cast({{ dq_analytical_string_literal(domain) }} as {{ dbt.type_string() }}) as domain
        , cast({{ dq_analytical_string_literal(metric) }} as {{ dbt.type_string() }}) as metric
        , cast(null as {{ dbt.type_numeric() }}) as result
    from (
        {{ dq_source_dimension_sql(source_relation) }}
    ) as sources
{% endmacro %}

{% macro dq_analytical_encounter_visits_per_1000_sql(domain, metric, encounter_type) %}
    {% set core_encounter_rel = dq_analytical_relation('core__encounter') %}
    {% set core_member_months_rel = dq_analytical_relation('core__member_months') %}

    {% if execute and core_encounter_rel is not none and core_member_months_rel is not none %}
        {% set metric_sql %}
            select
                  member_month_totals.data_source_key
                , case
                    when member_month_totals.member_months = 0 then 0
                    else (
                        cast(coalesce(encounter_counts.encounter_count, 0) as {{ dbt.type_numeric() }})
                        / cast(member_month_totals.member_months as {{ dbt.type_numeric() }})
                    ) * 12000
                  end as result
            from (
                select
                      coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                    , count(*) as member_months
                from {{ core_member_months_rel }}
                group by 1
            ) as member_month_totals
            left join (
                select
                      coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                    , count(*) as encounter_count
                from {{ core_encounter_rel }}
                where encounter_type = {{ dq_analytical_string_literal(encounter_type) }}
                group by 1
            ) as encounter_counts
                on member_month_totals.data_source_key = encounter_counts.data_source_key
        {% endset %}

        {{ dq_analytical_metric_select_sql(
            core_member_months_rel,
            domain,
            metric,
            metric_sql,
            "coalesce(metric_results.result, 0)"
        ) }}
    {% else %}
        {{ dq_analytical_empty_result_sql() }}
    {% endif %}
{% endmacro %}

{% macro dq_analytical_encounter_days_per_1000_sql(domain, metric, encounter_type) %}
    {% set core_encounter_rel = dq_analytical_relation('core__encounter') %}
    {% set core_member_months_rel = dq_analytical_relation('core__member_months') %}

    {% if execute and core_encounter_rel is not none and core_member_months_rel is not none %}
        {% set metric_sql %}
            select
                  member_month_totals.data_source_key
                , case
                    when member_month_totals.member_months = 0 then 0
                    else (
                        cast(coalesce(encounter_days.total_days, 0) as {{ dbt.type_numeric() }})
                        / cast(member_month_totals.member_months as {{ dbt.type_numeric() }})
                    ) * 12000
                  end as result
            from (
                select
                      coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                    , count(*) as member_months
                from {{ core_member_months_rel }}
                group by 1
            ) as member_month_totals
            left join (
                select
                      coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                    , coalesce(sum(length_of_stay), 0) as total_days
                from {{ core_encounter_rel }}
                where encounter_type = {{ dq_analytical_string_literal(encounter_type) }}
                group by 1
            ) as encounter_days
                on member_month_totals.data_source_key = encounter_days.data_source_key
        {% endset %}

        {{ dq_analytical_metric_select_sql(
            core_member_months_rel,
            domain,
            metric,
            metric_sql,
            "coalesce(metric_results.result, 0)"
        ) }}
    {% else %}
        {{ dq_analytical_empty_result_sql() }}
    {% endif %}
{% endmacro %}

{% macro dq_analytical_encounter_average_length_of_stay_sql(domain, metric, encounter_type) %}
    {% set core_encounter_rel = dq_analytical_relation('core__encounter') %}

    {% if execute and core_encounter_rel is not none %}
        {% set metric_sql %}
            select
                  coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                , avg(cast(length_of_stay as {{ dbt.type_numeric() }})) as result
            from {{ core_encounter_rel }}
            where encounter_type = {{ dq_analytical_string_literal(encounter_type) }}
            group by 1
        {% endset %}

        {{ dq_analytical_metric_select_sql(
            core_encounter_rel,
            domain,
            metric,
            metric_sql,
            "coalesce(metric_results.result, 0)"
        ) }}
    {% else %}
        {{ dq_analytical_empty_result_sql() }}
    {% endif %}
{% endmacro %}

{% macro dq_analytical_encounter_average_paid_amount_sql(domain, metric, encounter_type) %}
    {% set core_encounter_rel = dq_analytical_relation('core__encounter') %}

    {% if execute and core_encounter_rel is not none %}
        {% set metric_sql %}
            select
                  coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                , avg(cast(paid_amount as {{ dbt.type_numeric() }})) as result
            from {{ core_encounter_rel }}
            where encounter_type = {{ dq_analytical_string_literal(encounter_type) }}
            group by 1
        {% endset %}

        {{ dq_analytical_metric_select_sql(
            core_encounter_rel,
            domain,
            metric,
            metric_sql,
            "coalesce(metric_results.result, 0)"
        ) }}
    {% else %}
        {{ dq_analytical_empty_result_sql() }}
    {% endif %}
{% endmacro %}

{% macro dq_analytical_acute_inpatient_mortality_rate_sql(domain, metric) %}
    {% set core_encounter_rel = dq_analytical_relation('core__encounter') %}

    {% if execute and core_encounter_rel is not none %}
        {% set metric_sql %}
            select
                  coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                , case
                    when count(*) = 0 then 0
                    else (
                        cast(sum(
                            case
                                when discharge_disposition_code in ('20', '40', '41', '42') then 1
                                else 0
                            end
                        ) as {{ dbt.type_numeric() }})
                        / cast(count(*) as {{ dbt.type_numeric() }})
                    ) * 100
                  end as result
            from {{ core_encounter_rel }}
            where encounter_type = 'acute inpatient'
              and discharge_disposition_code is not null
              and encounter_end_date is not null
            group by 1
        {% endset %}

        {{ dq_analytical_metric_select_sql(
            core_encounter_rel,
            domain,
            metric,
            metric_sql,
            "coalesce(metric_results.result, 0)"
        ) }}
    {% else %}
        {{ dq_analytical_empty_result_sql() }}
    {% endif %}
{% endmacro %}

{% macro dq_analytical_pmpm_metric_sql(metric, value_column) %}
    {% set financial_pmpm_rel = dq_analytical_relation('financial_pmpm__pmpm_prep') %}

    {% if execute and financial_pmpm_rel is not none %}
        {% set metric_sql %}
            select
                  coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                , avg(cast({{ quote_column(value_column) }} as {{ dbt.type_numeric() }})) as result
            from {{ financial_pmpm_rel }}
            group by 1
        {% endset %}

        {{ dq_analytical_metric_select_sql(
            financial_pmpm_rel,
            'pmpm',
            metric,
            metric_sql,
            "coalesce(metric_results.result, 0)"
        ) }}
    {% else %}
        {{ dq_analytical_empty_result_sql() }}
    {% endif %}
{% endmacro %}

{% macro dq_analytical_total_member_months_sql(metric) %}
    {% set core_member_months_rel = dq_analytical_relation('core__member_months') %}

    {% if execute and core_member_months_rel is not none %}
        {% set metric_sql %}
            select
                  coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                , count(*) as result
            from {{ core_member_months_rel }}
            group by 1
        {% endset %}

        {{ dq_analytical_metric_select_sql(
            core_member_months_rel,
            'member months',
            metric,
            metric_sql,
            "coalesce(metric_results.result, 0)"
        ) }}
    {% else %}
        {{ dq_analytical_empty_result_sql() }}
    {% endif %}
{% endmacro %}

{% macro dq_analytical_average_member_months_sql(metric) %}
    {% set core_member_months_rel = dq_analytical_relation('core__member_months') %}

    {% if execute and core_member_months_rel is not none %}
        {% set metric_sql %}
            select
                  patient_counts.data_source_key
                , avg(cast(patient_counts.member_month_count as {{ dbt.type_numeric() }})) as result
            from (
                select
                      coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                    , person_id
                    , count(*) as member_month_count
                from {{ core_member_months_rel }}
                group by 1, 2
            ) as patient_counts
            group by 1
        {% endset %}

        {{ dq_analytical_metric_select_sql(
            core_member_months_rel,
            'member months',
            metric,
            metric_sql,
            "coalesce(metric_results.result, 0)"
        ) }}
    {% else %}
        {{ dq_analytical_empty_result_sql() }}
    {% endif %}
{% endmacro %}

{% macro dq_analytical_max_member_months_sql(metric) %}
    {% set core_member_months_rel = dq_analytical_relation('core__member_months') %}

    {% if execute and core_member_months_rel is not none %}
        {% set metric_sql %}
            select
                  patient_counts.data_source_key
                , max(patient_counts.member_month_count) as result
            from (
                select
                      coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                    , person_id
                    , count(*) as member_month_count
                from {{ core_member_months_rel }}
                group by 1, 2
            ) as patient_counts
            group by 1
        {% endset %}

        {{ dq_analytical_metric_select_sql(
            core_member_months_rel,
            'member months',
            metric,
            metric_sql,
            "coalesce(metric_results.result, 0)"
        ) }}
    {% else %}
        {{ dq_analytical_empty_result_sql() }}
    {% endif %}
{% endmacro %}

{% macro dq_analytical_members_with_claims_without_enrollment_sql(metric) %}
    {% set core_medical_claim_rel = dq_analytical_relation('core__medical_claim') %}
    {% set core_pharmacy_claim_rel = dq_analytical_relation('core__pharmacy_claim') %}
    {% set core_eligibility_rel = dq_analytical_relation('core__eligibility') %}
    {% set claims_member_queries = [] %}

    {% if core_medical_claim_rel is not none %}
        {% set medical_claim_members_query %}
            select distinct
                  cast(data_source as {{ dbt.type_string() }}) as data_source
                , person_id
            from {{ core_medical_claim_rel }}
        {% endset %}
        {% do claims_member_queries.append(medical_claim_members_query) %}
    {% endif %}

    {% if core_pharmacy_claim_rel is not none %}
        {% set pharmacy_claim_members_query %}
            select distinct
                  cast(data_source as {{ dbt.type_string() }}) as data_source
                , person_id
            from {{ core_pharmacy_claim_rel }}
        {% endset %}
        {% do claims_member_queries.append(pharmacy_claim_members_query) %}
    {% endif %}

    {% if execute and claims_member_queries | length > 0 and core_eligibility_rel is not none %}
        select
              sources.data_source
            , cast('member months' as {{ dbt.type_string() }}) as domain
            , cast({{ dq_analytical_string_literal(metric) }} as {{ dbt.type_string() }}) as metric
            , cast({{ dq_analytical_count_result_sql("coalesce(missing_members.result, 0)") }} as {{ dbt.type_numeric() }}) as result
        from (
            select distinct
                  source_rows.data_source_key
                , source_rows.data_source
            from (
                {% if core_medical_claim_rel is not none and core_pharmacy_claim_rel is not none %}
                    {{ dq_source_dimension_sql(core_medical_claim_rel) }}
                    union all
                    {{ dq_source_dimension_sql(core_pharmacy_claim_rel) }}
                {% elif core_medical_claim_rel is not none %}
                    {{ dq_source_dimension_sql(core_medical_claim_rel) }}
                {% else %}
                    {{ dq_source_dimension_sql(core_pharmacy_claim_rel) }}
                {% endif %}
            ) as source_rows
        ) as sources
        left join (
            select
                  claim_members.data_source_key
                , count(distinct claim_members.person_id) as result
            from (
                select
                      coalesce(data_source, '{{ dq_source_key_sentinel() }}') as data_source_key
                    , data_source
                    , person_id
                from (
                    {{ claims_member_queries | join('\nunion\n') }}
                ) as claim_members
            ) as claim_members
            left join (
                select distinct
                      coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                    , person_id
                from {{ core_eligibility_rel }}
            ) as eligible_members
                on claim_members.data_source_key = eligible_members.data_source_key
                and claim_members.person_id = eligible_members.person_id
            where eligible_members.person_id is null
            group by 1
        ) as missing_members
            on sources.data_source_key = missing_members.data_source_key
    {% else %}
        {{ dq_analytical_empty_result_sql() }}
    {% endif %}
{% endmacro %}

{% macro dq_analytical_ed_classification_percentage_sql(metric, classification) %}
    {% set ed_classification_rel = dq_analytical_relation('ed_classification__summary') %}

    {% if execute and ed_classification_rel is not none %}
        {% set classification_condition %}
            {% if classification is none %}
                ed_classification_description is null
            {% else %}
                ed_classification_description = {{ dq_analytical_string_literal(classification) }}
            {% endif %}
        {% endset %}

        {% set metric_sql %}
            select
                  totals.data_source_key
                , case
                    when totals.total_encounters = 0 then 0
                    else (
                        cast(coalesce(classified.encounters, 0) as {{ dbt.type_numeric() }})
                        / cast(totals.total_encounters as {{ dbt.type_numeric() }})
                    ) * 100
                  end as result
            from (
                select
                      coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                    , count(*) as total_encounters
                from {{ ed_classification_rel }}
                group by 1
            ) as totals
            left join (
                select
                      coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                    , count(*) as encounters
                from {{ ed_classification_rel }}
                where {{ classification_condition }}
                group by 1
            ) as classified
                on totals.data_source_key = classified.data_source_key
        {% endset %}

        {{ dq_analytical_metric_select_sql(
            ed_classification_rel,
            'emergency department',
            metric,
            metric_sql,
            "coalesce(metric_results.result, 0)"
        ) }}
    {% else %}
        {{ dq_analytical_empty_result_sql() }}
    {% endif %}
{% endmacro %}

{% macro dq_analytical_patient_count_sql(metric, where_sql=none) %}
    {% set core_patient_rel = dq_analytical_relation('core__patient') %}

    {% if execute and core_patient_rel is not none %}
        {% set metric_sql %}
            select
                  coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                , count(distinct person_id) as result
            from {{ core_patient_rel }}
            {% if where_sql is not none %}
            where {{ where_sql }}
            {% endif %}
            group by 1
        {% endset %}

        {{ dq_analytical_metric_select_sql(
            core_patient_rel,
            'patient demographics',
            metric,
            metric_sql,
            "coalesce(metric_results.result, 0)"
        ) }}
    {% else %}
        {{ dq_analytical_empty_result_sql() }}
    {% endif %}
{% endmacro %}

{% macro dq_analytical_patient_age_group_count_sql(metric, age_group) %}
    {{ dq_analytical_patient_count_sql(
        metric,
        "age_group = " ~ dq_analytical_string_literal(age_group)
    ) }}
{% endmacro %}

{% macro dq_analytical_patient_sex_count_sql(metric, sex_value) %}
    {{ dq_analytical_patient_count_sql(
        metric,
        "lower(cast(sex as " ~ dbt.type_string() ~ ")) = " ~ dq_analytical_string_literal(sex_value)
    ) }}
{% endmacro %}

{% macro dq_analytical_readmissions_acute_inpatient_visits_sql(metric) %}
    {{ dq_analytical_patient_count_sql(metric, "1 = 0") }}
{% endmacro %}

{% macro dq_analytical_acute_inpatient_count_sql(domain, metric) %}
    {% set core_encounter_rel = dq_analytical_relation('core__encounter') %}

    {% if execute and core_encounter_rel is not none %}
        {% set metric_sql %}
            select
                  coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                , count(*) as result
            from {{ core_encounter_rel }}
            where encounter_type = 'acute inpatient'
            group by 1
        {% endset %}

        {{ dq_analytical_metric_select_sql(
            core_encounter_rel,
            domain,
            metric,
            metric_sql,
            "coalesce(metric_results.result, 0)"
        ) }}
    {% else %}
        {{ dq_analytical_empty_result_sql() }}
    {% endif %}
{% endmacro %}

{% macro dq_analytical_readmissions_summary_count_sql(metric, flag_expression) %}
    {% set readmission_summary_rel = dq_analytical_relation('readmissions__readmission_summary') %}
    {% set readmission_augmented_rel = dq_analytical_relation('readmissions__encounter_augmented') %}

    {% if execute and readmission_summary_rel is not none and readmission_augmented_rel is not none %}
        {% set metric_sql %}
            select
                  coalesce(cast(augmented.data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                , sum(case when {{ flag_expression }} then 1 else 0 end) as result
            from {{ readmission_summary_rel }} as summary
            inner join {{ readmission_augmented_rel }} as augmented
                on summary.encounter_id = augmented.encounter_id
            group by 1
        {% endset %}

        {{ dq_analytical_metric_select_sql(
            readmission_augmented_rel,
            'readmissions',
            metric,
            metric_sql,
            "coalesce(metric_results.result, 0)"
        ) }}
    {% else %}
        {{ dq_analytical_empty_result_sql() }}
    {% endif %}
{% endmacro %}

{% macro dq_analytical_readmissions_rate_sql(metric, numerator_expression) %}
    {% set readmission_summary_rel = dq_analytical_relation('readmissions__readmission_summary') %}
    {% set readmission_augmented_rel = dq_analytical_relation('readmissions__encounter_augmented') %}

    {% if execute and readmission_summary_rel is not none and readmission_augmented_rel is not none %}
        {% set metric_sql %}
            select
                  readmission_counts.data_source_key
                , case
                    when readmission_counts.index_admissions = 0 then 0
                    else (
                        cast(readmission_counts.numerator as {{ dbt.type_numeric() }})
                        / cast(readmission_counts.index_admissions as {{ dbt.type_numeric() }})
                    ) * 100
                  end as result
            from (
                select
                      coalesce(cast(augmented.data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                    , sum(case when summary.index_admission_flag = 1 then 1 else 0 end) as index_admissions
                    , sum(case when {{ numerator_expression }} then 1 else 0 end) as numerator
                from {{ readmission_summary_rel }} as summary
                inner join {{ readmission_augmented_rel }} as augmented
                    on summary.encounter_id = augmented.encounter_id
                group by 1
            ) as readmission_counts
        {% endset %}

        {{ dq_analytical_metric_select_sql(
            readmission_augmented_rel,
            'readmissions',
            metric,
            metric_sql,
            "coalesce(metric_results.result, 0)"
        ) }}
    {% else %}
        {{ dq_analytical_empty_result_sql() }}
    {% endif %}
{% endmacro %}

{% macro dq_analytical_rate_of_index_admissions_sql(metric) %}
    {% set core_encounter_rel = dq_analytical_relation('core__encounter') %}
    {% set readmission_summary_rel = dq_analytical_relation('readmissions__readmission_summary') %}
    {% set readmission_augmented_rel = dq_analytical_relation('readmissions__encounter_augmented') %}

    {% if execute and core_encounter_rel is not none and readmission_summary_rel is not none and readmission_augmented_rel is not none %}
        {% set metric_sql %}
            select
                  acute_inpatient_counts.data_source_key
                , case
                    when acute_inpatient_counts.acute_inpatient_visits = 0 then 0
                    else (
                        cast(coalesce(index_counts.index_admissions, 0) as {{ dbt.type_numeric() }})
                        / cast(acute_inpatient_counts.acute_inpatient_visits as {{ dbt.type_numeric() }})
                    ) * 100
                  end as result
            from (
                select
                      coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                    , count(*) as acute_inpatient_visits
                from {{ core_encounter_rel }}
                where encounter_type = 'acute inpatient'
                group by 1
            ) as acute_inpatient_counts
            left join (
                select
                      coalesce(cast(augmented.data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                    , sum(case when summary.index_admission_flag = 1 then 1 else 0 end) as index_admissions
                from {{ readmission_summary_rel }} as summary
                inner join {{ readmission_augmented_rel }} as augmented
                    on summary.encounter_id = augmented.encounter_id
                group by 1
            ) as index_counts
                on acute_inpatient_counts.data_source_key = index_counts.data_source_key
        {% endset %}

        {{ dq_analytical_metric_select_sql(
            core_encounter_rel,
            'readmissions',
            metric,
            metric_sql,
            "coalesce(metric_results.result, 0)"
        ) }}
    {% else %}
        {{ dq_analytical_empty_result_sql() }}
    {% endif %}
{% endmacro %}

{% macro dq_analytical_chronic_condition_prevalence_sql(metric, source_condition_name=none) %}
    {% set chronic_conditions_rel = dq_analytical_relation('chronic_conditions__tuva_chronic_conditions_long') %}
    {% set core_patient_rel = dq_analytical_relation('core__patient') %}
    {% set chronic_condition_columns = dq_actual_columns(chronic_conditions_rel) if chronic_conditions_rel is not none else [] %}
    {% set patient_columns = dq_actual_columns(core_patient_rel) if core_patient_rel is not none else [] %}
    {% set join_on_data_source = dq_has_column(chronic_condition_columns, 'data_source') and dq_has_column(patient_columns, 'data_source') %}

    {% if execute and core_patient_rel is not none %}
        {% if source_condition_name is none %}
            {{ dq_analytical_metric_null_rows_sql(core_patient_rel, 'chronic conditions', metric) }}
        {% elif chronic_conditions_rel is not none %}
            {% set metric_sql %}
                select
                      patient_totals.data_source_key
                    , case
                        when patient_totals.total_patients = 0 then null
                        else (
                            cast(coalesce(condition_counts.patient_count, 0) as {{ dbt.type_numeric() }})
                            / cast(patient_totals.total_patients as {{ dbt.type_numeric() }})
                        ) * 100
                      end as result
                from (
                    select
                          coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                        , count(distinct person_id) as total_patients
                    from {{ core_patient_rel }}
                    group by 1
                ) as patient_totals
                left join (
                    select
                          coalesce(cast(patient.data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                        , count(distinct chronic_conditions.person_id) as patient_count
                    from {{ chronic_conditions_rel }} as chronic_conditions
                    inner join {{ core_patient_rel }} as patient
                        on chronic_conditions.person_id = patient.person_id
                       {% if join_on_data_source %}
                       and coalesce(cast(chronic_conditions.data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}')
                        = coalesce(cast(patient.data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}')
                       {% endif %}
                    where chronic_conditions.condition = {{ dq_analytical_string_literal(source_condition_name) }}
                    group by 1
                ) as condition_counts
                    on patient_totals.data_source_key = condition_counts.data_source_key
            {% endset %}

            {{ dq_analytical_metric_select_sql(
                core_patient_rel,
                'chronic conditions',
                metric,
                metric_sql,
                "metric_results.result"
            ) }}
        {% else %}
            {{ dq_analytical_empty_result_sql() }}
        {% endif %}
    {% else %}
        {{ dq_analytical_empty_result_sql() }}
    {% endif %}
{% endmacro %}

{% macro dq_analytical_medication_pmpm_sql(metric, brand_name, ingredient_name) %}
    {% set core_member_months_rel = dq_analytical_relation('core__member_months') %}
    {% set core_pharmacy_claim_rel = dq_analytical_relation('core__pharmacy_claim') %}
    {% set terminology_ndc_rel = dq_analytical_relation('terminology__ndc') %}
    {% set rxnorm_brand_generic_rel = dq_analytical_relation('terminology__rxnorm_brand_generic') %}

    {% if execute and core_member_months_rel is not none and core_pharmacy_claim_rel is not none and terminology_ndc_rel is not none and rxnorm_brand_generic_rel is not none %}
        {% set normalized_brand_name = dq_analytical_normalize_text_sql(dq_analytical_string_literal(brand_name)) %}
        {% set normalized_ingredient_name = dq_analytical_normalize_text_sql(dq_analytical_string_literal(ingredient_name)) %}

        {% set metric_sql %}
            with terminology_match as (
                select
                    case
                        when count(*) > 0 then 1
                        else 0
                    end as mapping_exists
                from {{ rxnorm_brand_generic_rel }}
                where {{ dq_analytical_normalize_text_sql('brand_name') }} = {{ normalized_brand_name }}
                  and {{ dq_analytical_normalize_text_sql('ingredient_name') }} = {{ normalized_ingredient_name }}
            )
            select
                  member_month_totals.data_source_key
                , case
                    when terminology_match.mapping_exists = 0 then null
                    when member_month_totals.member_months = 0 then null
                    else (
                        cast(coalesce(claim_totals.paid_amount, 0) as {{ dbt.type_numeric() }})
                        / cast(member_month_totals.member_months as {{ dbt.type_numeric() }})
                    )
                  end as result
            from (
                select
                      coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                    , count(*) as member_months
                from {{ core_member_months_rel }}
                group by 1
            ) as member_month_totals
            cross join terminology_match
            left join (
                select
                      coalesce(cast(pharmacy_claim.data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                    , coalesce(sum(pharmacy_claim.paid_amount), 0) as paid_amount
                from {{ core_pharmacy_claim_rel }} as pharmacy_claim
                inner join {{ terminology_ndc_rel }} as ndc
                    on pharmacy_claim.ndc_code = ndc.ndc
                inner join {{ rxnorm_brand_generic_rel }} as rxnorm_brand_generic
                    on ndc.rxcui = rxnorm_brand_generic.product_rxcui
                where {{ dq_analytical_normalize_text_sql('rxnorm_brand_generic.brand_name') }} = {{ normalized_brand_name }}
                  and {{ dq_analytical_normalize_text_sql('rxnorm_brand_generic.ingredient_name') }} = {{ normalized_ingredient_name }}
                group by 1
            ) as claim_totals
                on member_month_totals.data_source_key = claim_totals.data_source_key
        {% endset %}

        {{ dq_analytical_metric_select_sql(
            core_member_months_rel,
            'medications pmpm',
            metric,
            metric_sql,
            "metric_results.result"
        ) }}
    {% else %}
        {{ dq_analytical_empty_result_sql() }}
    {% endif %}
{% endmacro %}

{% macro dq_analytical_metric_model_sql(model_name) %}
    {% set spec = dq_analytical_metric_spec(model_name) %}
    {% set family = spec['family'] %}

    {% if family == 'encounter_visits_per_1000' %}
        {{ dq_analytical_encounter_visits_per_1000_sql(spec['domain'], spec['metric'], spec['encounter_type']) }}
    {% elif family == 'encounter_days_per_1000' %}
        {{ dq_analytical_encounter_days_per_1000_sql(spec['domain'], spec['metric'], spec['encounter_type']) }}
    {% elif family == 'encounter_average_length_of_stay' %}
        {{ dq_analytical_encounter_average_length_of_stay_sql(spec['domain'], spec['metric'], spec['encounter_type']) }}
    {% elif family == 'acute_inpatient_mortality_rate' %}
        {{ dq_analytical_acute_inpatient_mortality_rate_sql(spec['domain'], spec['metric']) }}
    {% elif family == 'encounter_average_paid_amount' %}
        {{ dq_analytical_encounter_average_paid_amount_sql(spec['domain'], spec['metric'], spec['encounter_type']) }}
    {% elif family == 'pmpm' %}
        {{ dq_analytical_pmpm_metric_sql(spec['metric'], spec['value_column']) }}
    {% elif family == 'total_member_months' %}
        {{ dq_analytical_total_member_months_sql(spec['metric']) }}
    {% elif family == 'members_with_claims_without_enrollment' %}
        {{ dq_analytical_members_with_claims_without_enrollment_sql(spec['metric']) }}
    {% elif family == 'average_member_months' %}
        {{ dq_analytical_average_member_months_sql(spec['metric']) }}
    {% elif family == 'max_member_months' %}
        {{ dq_analytical_max_member_months_sql(spec['metric']) }}
    {% elif family == 'ed_classification_percentage' %}
        {{ dq_analytical_ed_classification_percentage_sql(spec['metric'], spec['classification']) }}
    {% elif family == 'patient_count' %}
        {% if spec.get('where_sql') is not none %}
            {{ dq_analytical_patient_count_sql(spec['metric'], spec['where_sql']) }}
        {% else %}
            {{ dq_analytical_patient_count_sql(spec['metric']) }}
        {% endif %}
    {% elif family == 'patient_age_group_count' %}
        {{ dq_analytical_patient_age_group_count_sql(spec['metric'], spec['age_group']) }}
    {% elif family == 'patient_sex_count' %}
        {{ dq_analytical_patient_sex_count_sql(spec['metric'], spec['sex']) }}
    {% elif family == 'acute_inpatient_count' %}
        {{ dq_analytical_acute_inpatient_count_sql(spec['domain'], spec['metric']) }}
    {% elif family == 'readmissions_summary_count' %}
        {{ dq_analytical_readmissions_summary_count_sql(spec['metric'], spec['flag_expression']) }}
    {% elif family == 'readmissions_rate' %}
        {{ dq_analytical_readmissions_rate_sql(spec['metric'], spec['numerator_expression']) }}
    {% elif family == 'rate_of_index_admissions' %}
        {{ dq_analytical_rate_of_index_admissions_sql(spec['metric']) }}
    {% elif family == 'chronic_condition_prevalence' %}
        {{ dq_analytical_chronic_condition_prevalence_sql(spec['metric'], spec['source_condition']) }}
    {% elif family == 'medication_pmpm' %}
        {{ dq_analytical_medication_pmpm_sql(spec['metric'], spec['brand_name'], spec['ingredient_name']) }}
    {% else %}
        {{ exceptions.raise_compiler_error('Unsupported analytical metric family: ' ~ family ~ ' for ' ~ model_name) }}
    {% endif %}
{% endmacro %}
