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

{% macro dq_logical_investigation_sql(definition) %}
    {% set schema_prefix = var('tuva_schema_prefix', None) %}
    {% set input_schema = schema_prefix ~ '_input_layer' if schema_prefix is not none else 'input_layer' %}
    {% set flag_schema = schema_prefix ~ '_data_quality' if schema_prefix is not none else 'data_quality' %}
    {% set input_table_name = definition.get('input_table_name', definition['table_name']) %}
    {% set flag_table_name = definition.get('flag_table_name', definition['source_model_name'].replace('data_quality__', '')) %}
    {% set key_columns = definition.get('key_columns', ['data_source']) %}
    {% set join_conditions = [] %}

    {% for key_column in key_columns %}
        {% do join_conditions.append(
            "(source_rows." ~ key_column ~ " = flags." ~ key_column
            ~ " or (source_rows." ~ key_column ~ " is null and flags." ~ key_column ~ " is null))"
        ) %}
    {% endfor %}

    {{ return(
        "select\n"
        ~ "    source_rows.*\n"
        ~ "from " ~ input_schema ~ "." ~ input_table_name ~ " as source_rows\n"
        ~ "inner join " ~ flag_schema ~ "." ~ flag_table_name ~ " as flags\n"
        ~ "    on " ~ (join_conditions | join("\n   and ")) ~ "\n"
        ~ "where flags." ~ definition['flag_column_name'] ~ " = 1"
    ) }}
{% endmacro %}

{% macro dq_logical_test_manifest() %}
    {% set grouped_definitions = [
        {
            'source_model_name': 'data_quality__logical_flag_eligibility_span',
            'input_model_name': 'stg_input_layer__eligibility',
            'input_table_name': 'eligibility',
            'flag_table_name': 'logical_flag_eligibility_span',
            'table_name': 'eligibility',
            'grain': 'eligibility span',
            'key_columns': ['person_id', 'member_id', 'enrollment_start_date', 'enrollment_end_date', 'data_source'],
            'test_names': [
                'eligibility__sex_null',
                'eligibility__sex_invalid',
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
            'source_model_name': 'data_quality__logical_flag_eligibility_person',
            'input_model_name': 'stg_input_layer__eligibility',
            'input_table_name': 'eligibility',
            'flag_table_name': 'logical_flag_eligibility_person',
            'table_name': 'eligibility',
            'grain': 'person',
            'key_columns': ['person_id', 'data_source'],
            'test_names': [
                'eligibility__multiple_sexes_per_person',
                'eligibility__multiple_races_per_person',
                'eligibility__multiple_birth_dates_per_person'
            ]
        },
        {
            'source_model_name': 'data_quality__logical_flag_medical_claim_line',
            'input_model_name': 'stg_input_layer__medical_claim',
            'input_table_name': 'medical_claim',
            'flag_table_name': 'logical_flag_medical_claim_line',
            'table_name': 'medical_claim',
            'grain': 'medical claim line',
            'key_columns': ['claim_id', 'claim_line_number', 'data_source'],
            'test_names': [
                'medical_claim__claim_type_null',
                'medical_claim__claim_type_invalid',
                'medical_claim__institutional_indicators_present_for_professional_claim',
                'medical_claim__person_id_null',
                'medical_claim__claim_start_date_null',
                'medical_claim__claim_end_date_null',
                'medical_claim__claim_line_start_date_null',
                'medical_claim__claim_line_end_date_null',
                'medical_claim__claim_start_date_out_of_reasonable_range',
                'medical_claim__claim_end_date_out_of_reasonable_range',
                'medical_claim__claim_line_start_date_out_of_reasonable_range',
                'medical_claim__claim_line_end_date_out_of_reasonable_range',
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
            'source_model_name': 'data_quality__logical_flag_medical_claim_claim',
            'input_model_name': 'stg_input_layer__medical_claim',
            'input_table_name': 'medical_claim',
            'flag_table_name': 'logical_flag_medical_claim_claim',
            'table_name': 'medical_claim',
            'grain': 'medical claim',
            'key_columns': ['claim_id', 'data_source'],
            'test_names': [
                'medical_claim__claim_type_count_ne_one_per_claim',
                'medical_claim__multiple_person_ids_per_claim',
                'medical_claim__facility_npi_has_multiple_values_per_claim',
                'medical_claim__admission_date_has_multiple_values_per_inpatient_claim',
                'medical_claim__discharge_date_has_multiple_values_per_inpatient_claim',
                'medical_claim__bill_type_code_count_ne_one_for_institutional_claim',
                'medical_claim__drg_code_count_ne_one_for_acute_inpatient_claim',
                'medical_claim__diagnosis_code_count_gt_one_per_position_for_institutional_claim',
                'medical_claim__no_matching_eligibility_span'
            ]
        },
        {
            'source_model_name': 'data_quality__logical_flag_pharmacy_claim_line',
            'input_model_name': 'stg_input_layer__pharmacy_claim',
            'input_table_name': 'pharmacy_claim',
            'flag_table_name': 'logical_flag_pharmacy_claim_line',
            'table_name': 'pharmacy_claim',
            'grain': 'pharmacy claim line',
            'key_columns': ['claim_id', 'claim_line_number', 'data_source'],
            'test_names': [
                'pharmacy_claim__person_id_null',
                'pharmacy_claim__dispensing_date_null',
                'pharmacy_claim__paid_date_null',
                'pharmacy_claim__dispensing_date_out_of_reasonable_range',
                'pharmacy_claim__paid_date_out_of_reasonable_range',
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
            'source_model_name': 'data_quality__logical_flag_pharmacy_claim_claim',
            'input_model_name': 'stg_input_layer__pharmacy_claim',
            'input_table_name': 'pharmacy_claim',
            'flag_table_name': 'logical_flag_pharmacy_claim_claim',
            'table_name': 'pharmacy_claim',
            'grain': 'pharmacy claim',
            'key_columns': ['claim_id', 'data_source'],
            'test_names': [
                'pharmacy_claim__multiple_person_ids_per_claim',
                'pharmacy_claim__no_matching_eligibility_span'
            ]
        },
        {
            'source_model_name': 'data_quality__logical_flag_appointment',
            'input_model_name': 'stg_input_layer__appointment',
            'input_table_name': 'appointment',
            'flag_table_name': 'logical_flag_appointment',
            'table_name': 'appointment',
            'grain': 'appointment record',
            'key_columns': ['appointment_id', 'data_source'],
            'test_names': [
                'appointment__person_id_not_in_patient',
                'appointment__patient_id_not_in_patient',
                'appointment__encounter_id_not_in_encounter',
                'appointment__start_datetime_null'
            ]
        },
        {
            'source_model_name': 'data_quality__logical_flag_condition',
            'input_model_name': 'stg_input_layer__condition',
            'input_table_name': 'condition',
            'flag_table_name': 'logical_flag_condition',
            'table_name': 'condition',
            'grain': 'condition record',
            'key_columns': ['source_condition_id', 'data_source'],
            'test_names': [
                'condition__person_id_null',
                'condition__patient_id_null',
                'condition__source_code_null',
                'condition__code_system_null',
                'condition__person_id_not_in_patient',
                'condition__patient_id_not_in_patient',
                'condition__encounter_id_not_in_encounter',
                'condition__code_system_invalid',
                'condition__source_code_invalid',
                'condition__present_on_admit_code_invalid'
            ]
        },
        {
            'source_model_name': 'data_quality__logical_flag_encounter',
            'input_model_name': 'stg_input_layer__encounter',
            'input_table_name': 'encounter',
            'flag_table_name': 'logical_flag_encounter',
            'table_name': 'encounter',
            'grain': 'encounter record',
            'key_columns': ['encounter_id', 'data_source'],
            'test_names': [
                'encounter__person_id_null',
                'encounter__patient_id_null',
                'encounter__person_id_not_in_patient',
                'encounter__patient_id_not_in_patient',
                'encounter__encounter_type_invalid',
                'encounter__encounter_start_date_null',
                'encounter__encounter_end_date_null',
                'encounter__encounter_start_date_after_encounter_end_date',
                'encounter__encounter_start_date_out_of_reasonable_range',
                'encounter__encounter_end_date_out_of_reasonable_range',
                'encounter__admit_source_code_invalid',
                'encounter__admit_type_code_invalid',
                'encounter__discharge_disposition_code_invalid',
                'encounter__facility_npi_invalid',
                'encounter__primary_diagnosis_code_type_null',
                'encounter__primary_diagnosis_code_type_invalid',
                'encounter__primary_diagnosis_code_null',
                'encounter__primary_diagnosis_code_invalid',
                'encounter__drg_code_type_null',
                'encounter__drg_code_type_invalid',
                'encounter__drg_code_null',
                'encounter__drg_code_invalid'
            ]
        },
        {
            'source_model_name': 'data_quality__logical_flag_immunization',
            'input_model_name': 'stg_input_layer__immunization',
            'input_table_name': 'immunization',
            'flag_table_name': 'logical_flag_immunization',
            'table_name': 'immunization',
            'grain': 'immunization record',
            'key_columns': ['immunization_id', 'data_source'],
            'test_names': [
                'immunization__person_id_null',
                'immunization__patient_id_null',
                'immunization__person_id_not_in_patient',
                'immunization__patient_id_not_in_patient',
                'immunization__encounter_id_not_in_encounter'
            ]
        },
        {
            'source_model_name': 'data_quality__logical_flag_lab_result',
            'input_model_name': 'stg_input_layer__lab_result',
            'input_table_name': 'lab_result',
            'flag_table_name': 'logical_flag_lab_result',
            'table_name': 'lab_result',
            'grain': 'lab result record',
            'key_columns': ['lab_result_id', 'data_source'],
            'test_names': [
                'lab_result__person_id_null',
                'lab_result__patient_id_null',
                'lab_result__person_id_not_in_patient',
                'lab_result__patient_id_not_in_patient',
                'lab_result__encounter_id_not_in_encounter',
                'lab_result__accession_number_null',
                'lab_result__source_component_type_null_when_source_component_code_present',
                'lab_result__source_component_type_invalid',
                'lab_result__source_component_code_invalid'
            ]
        },
        {
            'source_model_name': 'data_quality__logical_flag_location',
            'input_model_name': 'stg_input_layer__location',
            'input_table_name': 'location',
            'flag_table_name': 'logical_flag_location',
            'table_name': 'location',
            'grain': 'location record',
            'key_columns': ['location_id', 'data_source'],
            'test_names': [
                'location__npi_invalid',
                'location__state_invalid',
                'location__zip_code_invalid_format'
            ]
        },
        {
            'source_model_name': 'data_quality__logical_flag_medication',
            'input_model_name': 'stg_input_layer__medication',
            'input_table_name': 'medication',
            'flag_table_name': 'logical_flag_medication',
            'table_name': 'medication',
            'grain': 'medication record',
            'key_columns': ['medication_id', 'data_source'],
            'test_names': [
                'medication__person_id_null',
                'medication__patient_id_null',
                'medication__person_id_not_in_patient',
                'medication__patient_id_not_in_patient',
                'medication__encounter_id_not_in_encounter',
                'medication__practitioner_id_not_in_practitioner',
                'medication__dispensing_date_out_of_range',
                'medication__prescribing_date_out_of_range',
                'medication__prescribing_date_after_dispensing_date',
                'medication__source_code_type_null_when_source_code_present',
                'medication__source_code_type_invalid',
                'medication__source_code_null',
                'medication__source_code_invalid',
                'medication__ndc_code_invalid',
                'medication__rxnorm_code_invalid',
                'medication__atc_code_invalid',
                'medication__quantity_negative',
                'medication__days_supply_negative'
            ]
        },
        {
            'source_model_name': 'data_quality__logical_flag_observation',
            'input_model_name': 'stg_input_layer__observation',
            'input_table_name': 'observation',
            'flag_table_name': 'logical_flag_observation',
            'table_name': 'observation',
            'grain': 'observation record',
            'key_columns': ['observation_id', 'data_source'],
            'test_names': [
                'observation__person_id_null',
                'observation__patient_id_null',
                'observation__person_id_not_in_patient',
                'observation__patient_id_not_in_patient',
                'observation__encounter_id_not_in_encounter',
                'observation__observation_date_null',
                'observation__observation_date_out_of_range',
                'observation__observation_type_invalid',
                'observation__source_code_type_null_when_source_code_present',
                'observation__source_code_type_invalid',
                'observation__source_code_null',
                'observation__source_code_invalid'
            ]
        },
        {
            'source_model_name': 'data_quality__logical_flag_patient',
            'input_model_name': 'stg_input_layer__patient',
            'input_table_name': 'patient',
            'flag_table_name': 'logical_flag_patient',
            'table_name': 'patient',
            'grain': 'patient record',
            'key_columns': ['person_id', 'patient_id', 'data_source'],
            'test_names': [
                'patient__sex_null',
                'patient__sex_invalid',
                'patient__race_invalid',
                'patient__ethnicity_invalid',
                'patient__birth_date_null',
                'patient__birth_date_out_of_range',
                'patient__death_date_out_of_range',
                'patient__birth_date_after_death_date',
                'patient__death_flag_invalid',
                'patient__death_flag_without_death_date',
                'patient__death_date_without_death_flag',
                'patient__state_invalid',
                'patient__zip_code_invalid_format',
                'patient__multiple_sexes_per_person',
                'patient__multiple_birth_dates_per_person'
            ]
        },
        {
            'source_model_name': 'data_quality__logical_flag_practitioner',
            'input_model_name': 'stg_input_layer__practitioner',
            'input_table_name': 'practitioner',
            'flag_table_name': 'logical_flag_practitioner',
            'table_name': 'practitioner',
            'grain': 'practitioner record',
            'key_columns': ['practitioner_id', 'data_source'],
            'test_names': [
                'practitioner__npi_invalid',
                'practitioner__npi_not_individual'
            ]
        },
        {
            'source_model_name': 'data_quality__logical_flag_procedure',
            'input_model_name': 'stg_input_layer__procedure',
            'input_table_name': 'procedure',
            'flag_table_name': 'logical_flag_procedure',
            'table_name': 'procedure',
            'grain': 'procedure record',
            'key_columns': ['source_procedure_id', 'data_source'],
            'test_names': [
                'procedure__person_id_null',
                'procedure__patient_id_null',
                'procedure__person_id_not_in_patient',
                'procedure__patient_id_not_in_patient',
                'procedure__encounter_id_not_in_encounter',
                'procedure__practitioner_id_not_in_practitioner',
                'procedure__procedure_date_null',
                'procedure__procedure_date_out_of_range',
                'procedure__code_system_null',
                'procedure__code_system_invalid',
                'procedure__source_code_null',
                'procedure__source_code_invalid'
            ]
        }
    ] %}

    {% set manifest = [] %}

    {% for definition in grouped_definitions %}
        {% for test_name in definition['test_names'] %}
            {% set registry_entry = {
                'source_model_name': definition['source_model_name'],
                'input_model_name': definition['input_model_name'],
                'input_table_name': definition['input_table_name'],
                'flag_table_name': definition['flag_table_name'],
                'table_name': definition['table_name'],
                'grain': definition['grain'],
                'key_columns': definition['key_columns'],
                'test_name': test_name,
                'flag_column_name': dq_logical_flag_column_name(test_name),
                'display_name': dq_logical_display_name(definition['table_name'], test_name),
                'description': dq_logical_test_description(definition['table_name'], test_name),
                'test_type': dq_logical_test_type(test_name),
                'severity': dq_logical_test_severity(test_name)
            } %}
            {% do registry_entry.update({
                'investigation_sql': dq_logical_investigation_sql(registry_entry)
            }) %}
            {% do manifest.append(registry_entry) %}
        {% endfor %}
    {% endfor %}

    {{ return(manifest) }}
{% endmacro %}

{% macro dq_logical_test_registry() %}
    {{ return(dq_logical_test_manifest()) }}
{% endmacro %}

{% macro dq_enabled_logical_test_manifest() %}
    {% set enabled_model_names = dq_enabled_input_layer_model_names() %}
    {% set filtered_manifest = [] %}

    {% for definition in dq_logical_test_manifest() %}
        {% if definition['input_model_name'] in enabled_model_names %}
            {% do filtered_manifest.append(definition) %}
        {% endif %}
    {% endfor %}

    {{ return(filtered_manifest) }}
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
        , '{{ definition['test_name'] }}' as test_name
        , '{{ definition['display_name'] }}' as display_name
        , cast(sum(cast(coalesce({{ quote_column(definition['flag_column_name']) }}, 0) as {{ dbt.type_int() }})) as {{ dbt.type_int() }}) as test_result
    from {{ ref(definition['source_model_name']) }}
    group by cast(data_source as {{ dbt.type_string() }})
{% endmacro %}
