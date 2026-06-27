{% macro dq_current_date_sql() %}
    {{ return("cast(" ~ dbt.current_timestamp() ~ " as " ~ api.Column.translate_type('date') ~ ")") }}
{% endmacro %}

{% macro dq_date_literal_sql(date_string) %}
    {{ return(dbt.cast("'" ~ date_string ~ "'", api.Column.translate_type('date'))) }}
{% endmacro %}

{% macro dq_logical_display_name(table_name, test_name) %}
    {% set display_names = {
        'eligibility__birth_date_after_death_date': 'birth_date after death_date',
        'eligibility__multiple_birth_dates_per_person': 'birth_date has multiple values per person_id',
        'eligibility__birth_date_null': 'birth_date null',
        'eligibility__birth_date_out_of_reasonable_range': 'birth_date out of reasonable range',
        'eligibility__death_flag_invalid': 'death flag invalid',
        'eligibility__death_flag_without_death_date': 'death_flag indicates death without death_date',
        'eligibility__death_date_out_of_reasonable_range': 'death_date out of reasonable range',
        'eligibility__enrollment_start_after_end': 'enrollment_start_date after enrollment_end_date',
        'eligibility__sex_invalid': 'sex invalid',
        'eligibility__multiple_sexes_per_person': 'sex has multiple values per person_id',
        'eligibility__sex_null': 'sex null',
        'eligibility__payer_type_invalid': 'payer_type invalid',
        'eligibility__payer_type_null': 'payer_type null',
        'eligibility__race_invalid': 'race invalid',
        'eligibility__multiple_races_per_person': 'race has multiple values per person_id',
        'eligibility__race_null': 'race null',
        'medical_claim__admission_date_after_discharge_date': 'admission_date after discharge_date',
        'medical_claim__admission_date_has_multiple_values_per_inpatient_claim': 'admission_date has multiple values per inpatient claim',
        'medical_claim__admission_date_out_of_reasonable_range': 'admission_date out of reasonable range',
        'medical_claim__admit_source_code_invalid': 'admit_source_code invalid',
        'medical_claim__admit_type_code_invalid': 'admit_type_code invalid',
        'medical_claim__allowed_amount_null': 'allowed_amount null',
        'medical_claim__allowed_amount_lt_zero': 'allowed_amount less than zero',
        'medical_claim__bill_type_code_count_ne_one_for_institutional_claim': 'bill_type_code has multiple values per institutional claim_id',
        'medical_claim__bill_type_code_invalid': 'bill_type_code invalid',
        'medical_claim__bill_type_code_null_for_institutional_claim': 'bill_type_code null for institutional claim',
        'medical_claim__billing_npi_invalid': 'billing_npi invalid',
        'medical_claim__billing_npi_null': 'billing_npi null',
        'medical_claim__claim_end_date_null': 'claim_end_date null',
        'medical_claim__claim_line_end_date_null': 'claim_line_end_date null',
        'medical_claim__claim_line_start_date_null': 'claim_line_start_date null',
        'medical_claim__claim_line_start_after_claim_line_end': 'claim_line_start_date after claim_line_end_date',
        'medical_claim__claim_start_after_claim_end': 'claim_start_date after claim_end_date',
        'medical_claim__claim_start_date_null': 'claim_start_date null',
        'medical_claim__claim_type_count_ne_one_per_claim': 'claim_type has multiple values per claim_id',
        'medical_claim__claim_type_invalid': 'claim_type invalid',
        'medical_claim__claim_type_null': 'claim_type null',
        'medical_claim__institutional_indicators_present_for_professional_claim': 'institutional indicators present for professional claim',
        'medical_claim__diagnosis_code_1_invalid': 'diagnosis_code_1 invalid',
        'medical_claim__diagnosis_code_1_null': 'diagnosis_code_1 null',
        'medical_claim__diagnosis_code_count_gt_one_per_position_for_institutional_claim': 'diagnosis_code has multiple values per position for institutional claim',
        'medical_claim__diagnosis_code_2_to_25_invalid': 'diagnosis_code_2 to diagnosis_code_25 invalid',
        'medical_claim__diagnosis_code_type_invalid': 'diagnosis_code_type invalid',
        'medical_claim__diagnosis_code_type_null_when_diagnosis_code_present': 'diagnosis_code_type null when diagnosis_code present',
        'medical_claim__discharge_disposition_code_invalid': 'discharge_disposition_code invalid',
        'medical_claim__discharge_date_has_multiple_values_per_inpatient_claim': 'discharge_date has multiple values per inpatient claim',
        'medical_claim__discharge_date_out_of_reasonable_range': 'discharge_date out of reasonable range',
        'medical_claim__drg_code_count_ne_one_for_acute_inpatient_claim': 'drg_code has multiple values per acute inpatient claim_id',
        'medical_claim__drg_code_invalid': 'drg_code invalid',
        'medical_claim__drg_code_null_for_acute_inpatient_claim': 'drg_code null for acute inpatient claim',
        'medical_claim__drg_code_type_invalid': 'drg_code_type invalid',
        'medical_claim__drg_code_type_null_when_drg_code_present': 'drg_code_type null when drg_code present',
        'medical_claim__admission_date_null_for_inpatient_claim': 'admission_date null for inpatient claim',
        'medical_claim__discharge_date_null_for_inpatient_claim': 'discharge_date null for inpatient claim',
        'medical_claim__facility_npi_invalid': 'facility_npi invalid',
        'medical_claim__facility_npi_null_for_inpatient_claim': 'facility_npi null for inpatient claim',
        'medical_claim__facility_npi_has_multiple_values_per_claim': 'facility_npi has multiple values per claim_id',
        'medical_claim__hcpcs_code_null_for_professional_claim': 'hcpcs_code null for professional claim',
        'medical_claim__no_matching_eligibility_span': 'no matching eligibility span',
        'medical_claim__paid_amount_null': 'paid_amount null',
        'medical_claim__paid_amount_gt_allowed_amount': 'paid_amount greater than allowed_amount',
        'medical_claim__paid_amount_lt_zero': 'paid_amount less than zero',
        'medical_claim__multiple_person_ids_per_claim': 'person_id has multiple values per claim',
        'medical_claim__person_id_null': 'person_id null',
        'medical_claim__place_of_service_code_invalid': 'place_of_service_code invalid',
        'medical_claim__place_of_service_code_present_for_institutional_claim': 'place_of_service_code present for institutional claim',
        'medical_claim__place_of_service_code_null_for_professional_claim': 'place_of_service_code null for professional claim',
        'medical_claim__procedure_code_1_to_25_invalid': 'procedure_code_1 to procedure_code_25 invalid',
        'medical_claim__procedure_code_type_invalid': 'procedure_code_type invalid',
        'medical_claim__procedure_code_type_null_when_procedure_code_present': 'procedure_code_type null when procedure_code present',
        'medical_claim__rendering_npi_invalid': 'rendering_npi invalid',
        'medical_claim__rendering_npi_null': 'rendering_npi null',
        'medical_claim__revenue_center_code_invalid': 'revenue_center_code invalid',
        'medical_claim__revenue_center_code_null_for_institutional_claim': 'revenue_center_code null for institutional claim',
        'pharmacy_claim__allowed_amount_null': 'allowed_amount null',
        'pharmacy_claim__allowed_amount_lt_zero': 'allowed_amount less than zero',
        'pharmacy_claim__dispensing_date_null': 'dispensing_date null',
        'pharmacy_claim__dispensing_provider_npi_invalid': 'dispensing_provider_npi invalid',
        'pharmacy_claim__dispensing_provider_npi_null': 'dispensing_provider_npi null',
        'pharmacy_claim__ndc_code_invalid': 'ndc_code invalid',
        'pharmacy_claim__ndc_code_null': 'ndc_code null',
        'pharmacy_claim__no_matching_eligibility_span': 'no matching eligibility span',
        'pharmacy_claim__paid_amount_null': 'paid_amount null',
        'pharmacy_claim__paid_amount_gt_allowed_amount': 'paid_amount greater than allowed_amount',
        'pharmacy_claim__paid_amount_lt_zero': 'paid_amount less than zero',
        'pharmacy_claim__paid_date_null': 'paid_date null',
        'pharmacy_claim__multiple_person_ids_per_claim': 'person_id has multiple values per claim',
        'pharmacy_claim__person_id_null': 'person_id null',
        'pharmacy_claim__prescribing_provider_npi_invalid': 'prescribing_provider_npi invalid',
        'pharmacy_claim__prescribing_provider_npi_null': 'prescribing_provider_npi null',
        'appointment__person_id_not_in_patient': 'person_id not found in patient',
        'appointment__patient_id_not_in_patient': 'patient_id not found in patient',
        'appointment__encounter_id_not_in_encounter': 'encounter_id not found in encounter',
        'appointment__start_datetime_null': 'start_datetime null',
        'condition__person_id_null': 'person_id null',
        'condition__patient_id_null': 'patient_id null',
        'condition__source_code_null': 'source_code null',
        'condition__code_system_null': 'code_system null',
        'condition__person_id_not_in_patient': 'person_id not found in patient',
        'condition__patient_id_not_in_patient': 'patient_id not found in patient',
        'condition__encounter_id_not_in_encounter': 'encounter_id not found in encounter',
        'condition__code_system_invalid': 'code_system invalid',
        'condition__source_code_invalid': 'source_code invalid',
        'condition__present_on_admit_code_invalid': 'present_on_admit_code invalid',
        'encounter__person_id_null': 'person_id null',
        'encounter__patient_id_null': 'patient_id null',
        'encounter__person_id_not_in_patient': 'person_id not found in patient',
        'encounter__patient_id_not_in_patient': 'patient_id not found in patient',
        'encounter__encounter_type_invalid': 'encounter_type invalid',
        'encounter__encounter_start_date_null': 'encounter_start_date null',
        'encounter__encounter_end_date_null': 'encounter_end_date null',
        'encounter__encounter_start_date_out_of_reasonable_range': 'encounter_start_date out of reasonable range',
        'encounter__encounter_end_date_out_of_reasonable_range': 'encounter_end_date out of reasonable range',
        'encounter__admit_source_code_invalid': 'admit_source_code invalid',
        'encounter__admit_type_code_invalid': 'admit_type_code invalid',
        'encounter__discharge_disposition_code_invalid': 'discharge_disposition_code invalid',
        'encounter__facility_npi_invalid': 'facility_npi invalid',
        'encounter__primary_diagnosis_code_type_null': 'primary_diagnosis_code_type null',
        'encounter__primary_diagnosis_code_type_invalid': 'primary_diagnosis_code_type invalid',
        'encounter__primary_diagnosis_code_null': 'primary_diagnosis_code null',
        'encounter__primary_diagnosis_code_invalid': 'primary_diagnosis_code invalid',
        'encounter__drg_code_type_null': 'drg_code_type null',
        'encounter__drg_code_type_invalid': 'drg_code_type invalid',
        'encounter__drg_code_null': 'drg_code null',
        'encounter__drg_code_invalid': 'drg_code invalid',
        'immunization__person_id_null': 'person_id null',
        'immunization__patient_id_null': 'patient_id null',
        'immunization__person_id_not_in_patient': 'person_id not found in patient',
        'immunization__patient_id_not_in_patient': 'patient_id not found in patient',
        'immunization__encounter_id_not_in_encounter': 'encounter_id not found in encounter',
        'lab_result__person_id_null': 'person_id null',
        'lab_result__patient_id_null': 'patient_id null',
        'lab_result__person_id_not_in_patient': 'person_id not found in patient',
        'lab_result__patient_id_not_in_patient': 'patient_id not found in patient',
        'lab_result__encounter_id_not_in_encounter': 'encounter_id not found in encounter',
        'lab_result__accession_number_null': 'accession_number null',
        'lab_result__source_component_type_invalid': 'source_component_type invalid',
        'lab_result__source_component_code_invalid': 'source_component_code invalid'
    } %}

    {{ return(display_names.get(test_name, test_name)) }}
{% endmacro %}

{% macro dq_logical_test_description(table_name, test_name) %}
    {% set display_name = dq_logical_display_name(table_name, test_name) %}
    {% set table_label = "input_layer." ~ table_name %}

    {% if test_name.endswith('__person_id_null') %}
        {{ return("Checks whether person_id is null in " ~ table_label ~ ".") }}
    {% elif test_name.endswith('__patient_id_null') %}
        {{ return("Checks whether patient_id is null in " ~ table_label ~ ".") }}
    {% elif test_name.endswith('__person_id_not_in_patient') %}
        {{ return("Checks whether person_id values in " ~ table_label ~ " have a corresponding person_id in input_layer.patient for the same data_source.") }}
    {% elif test_name.endswith('__patient_id_not_in_patient') %}
        {{ return("Checks whether patient_id values in " ~ table_label ~ " have a corresponding patient_id in input_layer.patient for the same data_source.") }}
    {% elif test_name.endswith('__encounter_id_not_in_encounter') %}
        {{ return("Checks whether populated encounter_id values in " ~ table_label ~ " have a corresponding encounter_id in input_layer.encounter for the same data_source.") }}
    {% elif test_name.endswith('__no_matching_eligibility_span') %}
        {{ return("Checks whether records in " ~ table_label ~ " have no matching eligibility span for the same person_id and data_source during the relevant claim or dispensing dates.") }}
    {% elif 'multiple_person_ids_per_claim' in test_name %}
        {{ return("Checks whether a claim_id in " ~ table_label ~ " is associated with more than one person_id within the same data_source.") }}
    {% elif 'multiple_' in test_name or '_has_multiple_values_' in test_name or '_count_ne_one_' in test_name or '_count_gt_one_' in test_name %}
        {{ return("Checks whether " ~ display_name ~ " in " ~ table_label ~ ".") }}
    {% elif test_name.endswith('_null') or '_null_' in test_name %}
        {{ return("Checks whether " ~ display_name ~ " in " ~ table_label ~ ".") }}
    {% elif test_name.endswith('_invalid') or '_invalid_' in test_name %}
        {{ return("Checks whether " ~ display_name ~ " in " ~ table_label ~ " by comparing populated values to Tuva's accepted values or terminology data assets.") }}
    {% elif '_out_of_reasonable_range' in test_name %}
        {{ return("Checks whether " ~ display_name ~ " in " ~ table_label ~ " using Tuva's configured healthcare data quality date range.") }}
    {% elif '_after_' in test_name %}
        {{ return("Checks whether " ~ display_name ~ " in " ~ table_label ~ ".") }}
    {% elif '_lt_zero' in test_name %}
        {{ return("Checks whether " ~ display_name ~ " in " ~ table_label ~ ".") }}
    {% elif '_gt_allowed_amount' in test_name %}
        {{ return("Checks whether paid_amount is greater than allowed_amount in " ~ table_label ~ ".") }}
    {% elif 'institutional_indicators_present_for_professional_claim' in test_name %}
        {{ return("Checks whether professional claims in " ~ table_label ~ " contain institutional-only fields such as bill type, revenue center, admit, discharge, or DRG fields.") }}
    {% elif 'present_for_institutional_claim' in test_name %}
        {{ return("Checks whether " ~ display_name ~ " in " ~ table_label ~ ".") }}
    {% elif 'null_for_institutional_claim' in test_name or 'null_for_professional_claim' in test_name or 'null_for_inpatient_claim' in test_name or 'null_for_acute_inpatient_claim' in test_name %}
        {{ return("Checks whether " ~ display_name ~ " in " ~ table_label ~ ".") }}
    {% else %}
        {{ return("Checks whether " ~ display_name ~ " in " ~ table_label ~ ".") }}
    {% endif %}
{% endmacro %}

{% macro dq_logical_source_key_expression_sql(relation, relation_alias='source_rows') %}
    {% set actual_columns = dq_actual_columns(relation) %}

    {% if dq_has_column(actual_columns, 'data_source') %}
        {{ return("coalesce(cast(" ~ relation_alias ~ ".data_source as " ~ dbt.type_string() ~ "), '" ~ dq_source_key_sentinel() ~ "')") }}
    {% else %}
        {{ return("'" ~ dq_source_key_sentinel() ~ "'") }}
    {% endif %}
{% endmacro %}

{% macro dq_has_any_columns_populated_sql(column_names, relation_alias='source_rows') %}
    {% set clauses = [] %}

    {% for column_name in column_names %}
        {% do clauses.append(relation_alias ~ "." ~ quote_column(column_name) ~ " is not null") %}
    {% endfor %}

    {{ return("(" ~ clauses | join(" or ") ~ ")") }}
{% endmacro %}

{% macro dq_medical_claim_inpatient_facility_where_sql(relation_alias='source_rows') %}
    {% set bill_type_prefix_expression = substring("cast(" ~ relation_alias ~ ".bill_type_code as " ~ dbt.type_string() ~ ")", 1, 2) %}

    {{ return(
        "lower(cast(" ~ relation_alias ~ ".claim_type as " ~ dbt.type_string() ~ ")) = 'institutional'"
        ~ " and " ~ relation_alias ~ ".bill_type_code is not null"
        ~ " and " ~ bill_type_prefix_expression ~ " in ('11', '12', '15', '16', '17', '18', '21', '22', '25', '26', '27', '28', '31', '41', '42', '45', '46', '47', '48', '61', '62', '65', '66', '67', '68', '82')"
    ) }}
{% endmacro %}

{% macro dq_medical_claim_acute_inpatient_where_sql(relation_alias='source_rows') %}
    {% set bill_type_prefix_expression = substring("cast(" ~ relation_alias ~ ".bill_type_code as " ~ dbt.type_string() ~ ")", 1, 2) %}

    {{ return(
        "lower(cast(" ~ relation_alias ~ ".claim_type as " ~ dbt.type_string() ~ ")) = 'institutional'"
        ~ " and " ~ relation_alias ~ ".bill_type_code is not null"
        ~ " and " ~ bill_type_prefix_expression ~ " in ('11', '12')"
    ) }}
{% endmacro %}
