{% macro dq_logical_flag_column_name(test_name) %}
    {% set parts = test_name.split('__', 1) %}
    {% if parts | length == 2 %}
        {{ return(parts[1]) }}
    {% endif %}

    {{ return(test_name) }}
{% endmacro %}

{% macro dq_logical_int_flag_sql(predicate_sql) %}
    {{ return("cast(case when " ~ predicate_sql ~ " then 1 else 0 end as " ~ dbt.type_int() ~ ")") }}
{% endmacro %}

{% macro dq_logical_test_manifest() %}
    {% set grouped_definitions = [
        {
            'source_model_name': 'data_quality__eligibility_span_flags',
            'table_name': 'eligibility',
            'test_names': [
                'eligibility__gender_null',
                'eligibility__gender_invalid',
                'eligibility__race_null',
                'eligibility__race_invalid',
                'eligibility__birth_date_null',
                'eligibility__birth_date_after_death_date',
                'eligibility__birth_date_out_of_reasonable_range',
                'eligibility__death_date_out_of_reasonable_range',
                'eligibility__death_flag_invalid',
                'eligibility__death_flag_without_death_date',
                'eligibility__enrollment_start_after_end',
                'eligibility__payer_type_null',
                'eligibility__payer_type_invalid'
            ]
        },
        {
            'source_model_name': 'data_quality__eligibility_person_flags',
            'table_name': 'eligibility',
            'test_names': [
                'eligibility__multiple_genders_per_person',
                'eligibility__multiple_races_per_person',
                'eligibility__multiple_birth_dates_per_person'
            ]
        },
        {
            'source_model_name': 'data_quality__medical_claim_line_flags',
            'table_name': 'medical_claim',
            'test_names': [
                'medical_claim__claim_type_null',
                'medical_claim__claim_type_invalid',
                'medical_claim__institutional_indicators_present_for_professional_claim',
                'medical_claim__person_id_null',
                'medical_claim__claim_start_date_null',
                'medical_claim__claim_end_date_null',
                'medical_claim__claim_line_start_date_null',
                'medical_claim__claim_line_end_date_null',
                'medical_claim__claim_start_after_claim_end',
                'medical_claim__claim_line_start_after_claim_line_end',
                'medical_claim__admission_date_after_discharge_date',
                'medical_claim__admission_date_out_of_reasonable_range',
                'medical_claim__admission_date_null_for_inpatient_claim',
                'medical_claim__discharge_date_null_for_inpatient_claim',
                'medical_claim__discharge_date_out_of_reasonable_range',
                'medical_claim__paid_amount_null',
                'medical_claim__paid_amount_lt_zero',
                'medical_claim__allowed_amount_null',
                'medical_claim__allowed_amount_lt_zero',
                'medical_claim__paid_amount_gt_allowed_amount',
                'medical_claim__admit_source_code_invalid',
                'medical_claim__admit_type_code_invalid',
                'medical_claim__discharge_disposition_code_invalid',
                'medical_claim__place_of_service_code_invalid',
                'medical_claim__bill_type_code_invalid',
                'medical_claim__revenue_center_code_invalid',
                'medical_claim__place_of_service_code_null_for_professional_claim',
                'medical_claim__place_of_service_code_present_for_institutional_claim',
                'medical_claim__bill_type_code_null_for_institutional_claim',
                'medical_claim__revenue_center_code_null_for_institutional_claim',
                'medical_claim__hcpcs_code_null_for_professional_claim',
                'medical_claim__rendering_npi_invalid',
                'medical_claim__billing_npi_invalid',
                'medical_claim__facility_npi_invalid',
                'medical_claim__rendering_npi_null',
                'medical_claim__billing_npi_null',
                'medical_claim__facility_npi_null_for_inpatient_claim',
                'medical_claim__drg_code_type_null_when_drg_code_present',
                'medical_claim__drg_code_type_invalid',
                'medical_claim__drg_code_invalid',
                'medical_claim__drg_code_null_for_acute_inpatient_claim',
                'medical_claim__diagnosis_code_1_null',
                'medical_claim__diagnosis_code_type_null_when_diagnosis_code_present',
                'medical_claim__diagnosis_code_type_invalid',
                'medical_claim__diagnosis_code_1_invalid',
                'medical_claim__diagnosis_code_2_to_25_invalid',
                'medical_claim__procedure_code_type_null_when_procedure_code_present',
                'medical_claim__procedure_code_type_invalid',
                'medical_claim__procedure_code_1_to_25_invalid'
            ]
        },
        {
            'source_model_name': 'data_quality__medical_claim_claim_flags',
            'table_name': 'medical_claim',
            'test_names': [
                'medical_claim__claim_type_count_ne_one_per_claim',
                'medical_claim__multiple_person_ids_per_claim',
                'medical_claim__admission_date_has_multiple_values_per_inpatient_claim',
                'medical_claim__discharge_date_has_multiple_values_per_inpatient_claim',
                'medical_claim__bill_type_code_count_ne_one_for_institutional_claim',
                'medical_claim__drg_code_count_ne_one_for_acute_inpatient_claim',
                'medical_claim__no_matching_eligibility_span'
            ]
        },
        {
            'source_model_name': 'data_quality__pharmacy_claim_line_flags',
            'table_name': 'pharmacy_claim',
            'test_names': [
                'pharmacy_claim__person_id_null',
                'pharmacy_claim__dispensing_date_null',
                'pharmacy_claim__paid_date_null',
                'pharmacy_claim__prescribing_provider_npi_null',
                'pharmacy_claim__prescribing_provider_npi_invalid',
                'pharmacy_claim__dispensing_provider_npi_null',
                'pharmacy_claim__dispensing_provider_npi_invalid',
                'pharmacy_claim__ndc_code_null',
                'pharmacy_claim__ndc_code_invalid',
                'pharmacy_claim__paid_amount_null',
                'pharmacy_claim__paid_amount_lt_zero',
                'pharmacy_claim__allowed_amount_null',
                'pharmacy_claim__allowed_amount_lt_zero',
                'pharmacy_claim__paid_amount_gt_allowed_amount'
            ]
        },
        {
            'source_model_name': 'data_quality__pharmacy_claim_claim_flags',
            'table_name': 'pharmacy_claim',
            'test_names': [
                'pharmacy_claim__multiple_person_ids_per_claim',
                'pharmacy_claim__no_matching_eligibility_span'
            ]
        }
    ] %}

    {% set manifest = [] %}

    {% for definition in grouped_definitions %}
        {% for test_name in definition['test_names'] %}
            {% do manifest.append({
                'source_model_name': definition['source_model_name'],
                'table_name': definition['table_name'],
                'test_name': test_name,
                'flag_column_name': dq_logical_flag_column_name(test_name),
                'display_name': dq_logical_display_name(definition['table_name'], test_name)
            }) %}
        {% endfor %}
    {% endfor %}

    {{ return(manifest) }}
{% endmacro %}

{% macro dq_logical_test_manifest_for_model(source_model_name) %}
    {% set filtered_manifest = [] %}

    {% for definition in dq_logical_test_manifest() %}
        {% if definition['source_model_name'] == source_model_name %}
            {% do filtered_manifest.append(definition) %}
        {% endif %}
    {% endfor %}

    {{ return(filtered_manifest) }}
{% endmacro %}

{% macro dq_logical_sum_flag_query_sql(definition) %}
    select
          cast(data_source as {{ dbt.type_string() }}) as data_source
        , '{{ definition['table_name'] }}' as {{ adapter.quote('table') }}
        , '{{ definition['display_name'] }}' as test_name
        , cast(sum(cast(coalesce({{ quote_column(definition['flag_column_name']) }}, 0) as {{ dbt.type_int() }})) as {{ dbt.type_int() }}) as test_result
    from {{ ref(definition['source_model_name']) }}
    group by cast(data_source as {{ dbt.type_string() }})
{% endmacro %}
