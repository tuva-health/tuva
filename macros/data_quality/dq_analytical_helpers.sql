{% macro dq_data_quality_schema_name() %}
    {% if var('tuva_schema_prefix', None) is not none %}
        {{ return(var('tuva_schema_prefix', None) ~ '_data_quality') }}
    {% endif %}

    {{ return('data_quality') }}
{% endmacro %}

{% macro dq_config_analytical_metric_model(alias_name) %}
    {{ config(
         schema = dq_data_quality_schema_name(),
         alias = alias_name,
         tags = ['data_quality', 'dqi', 'dq2', 'dq_analytical'],
         materialized = 'table'
       )
    }}
{% endmacro %}

{% macro dq_analytical_empty_result_sql() %}
    select
          cast(null as {{ dbt.type_string() }}) as data_source
        , cast(null as {{ dbt.type_string() }}) as domain
        , cast(null as {{ dbt.type_string() }}) as metric
        , cast(null as {{ dbt.type_numeric() }}) as result
    where 1 = 0
{% endmacro %}

{% macro dq_analytical_count_result_sql(result_expression) %}
    {{ return(
        "cast(round(cast(" ~ result_expression ~ " as " ~ dbt.type_numeric() ~ "), 0) as " ~ dbt.type_int() ~ ")"
    ) }}
{% endmacro %}

{% macro dq_analytical_decimal_result_sql(result_expression) %}
    {{ return(
        "cast(round(cast(" ~ result_expression ~ " as " ~ dbt.type_numeric() ~ "), 2) as " ~ dbt.type_numeric() ~ ")"
    ) }}
{% endmacro %}

{% macro dq_analytical_relation(model_name) %}
    {{ return(dq_actual_relation(dq_find_model_node(model_name))) }}
{% endmacro %}

{% macro dq_analytical_key_metric_model_names() %}
    {{ return([
        'data_quality__analytical_key_metric__count_distinct_patients',
        'data_quality__analytical_key_metric__count_distinct_patients_by_sex',
        'data_quality__analytical_key_metric__count_distinct_patients_by_age_group',
        'data_quality__analytical_key_metric__count_dead',
        'data_quality__analytical_key_metric__sum_medical_claim_paid_amount',
        'data_quality__analytical_key_metric__sum_medical_claim_paid_amount_by_claim_type',
        'data_quality__analytical_key_metric__sum_medical_claim_allowed_amount',
        'data_quality__analytical_key_metric__sum_medical_claim_allowed_amount_by_claim_type',
        'data_quality__analytical_key_metric__count_distinct_medical_claims',
        'data_quality__analytical_key_metric__count_distinct_medical_claims_by_claim_type',
        'data_quality__analytical_key_metric__sum_pharmacy_claim_paid_amount',
        'data_quality__analytical_key_metric__sum_pharmacy_claim_allowed_amount',
        'data_quality__analytical_key_metric__total_member_months',
        'data_quality__analytical_key_metric__avg_member_months',
        'data_quality__analytical_key_metric__max_member_months',
        'data_quality__analytical_key_metric__members_with_claims_without_enrollment',
        'data_quality__analytical_key_metric__members_with_claims_missing_enrollment_month',
        'data_quality__analytical_key_metric__acute_inpatient_visits_per_1000',
        'data_quality__analytical_key_metric__snf_visits_per_1000',
        'data_quality__analytical_key_metric__ed_visits_per_1000',
        'data_quality__analytical_key_metric__office_visits_per_1000',
        'data_quality__analytical_key_metric__inpatient_alos',
        'data_quality__analytical_key_metric__inpatient_mortality_rate',
        'data_quality__analytical_key_metric__index_admissions',
        'data_quality__analytical_key_metric__readmissions_30_day',
        'data_quality__analytical_key_metric__readmission_rate_30_day',
        'data_quality__analytical_key_metric__ed_classification_percentages',
        'data_quality__analytical_key_metric__top_chronic_condition_prevalence',
        'data_quality__analytical_key_metric__top_hcc_prevalence'
    ]) }}
{% endmacro %}
