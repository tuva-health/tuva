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
        'medical_claim__claim_end_date_out_of_reasonable_range': 'claim_end_date out of reasonable range',
        'medical_claim__claim_line_end_date_null': 'claim_line_end_date null',
        'medical_claim__claim_line_end_date_out_of_reasonable_range': 'claim_line_end_date out of reasonable range',
        'medical_claim__claim_line_start_date_null': 'claim_line_start_date null',
        'medical_claim__claim_line_start_date_out_of_reasonable_range': 'claim_line_start_date out of reasonable range',
        'medical_claim__claim_line_start_after_claim_line_end': 'claim_line_start_date after claim_line_end_date',
        'medical_claim__claim_start_after_claim_end': 'claim_start_date after claim_end_date',
        'medical_claim__claim_start_date_null': 'claim_start_date null',
        'medical_claim__claim_start_date_out_of_reasonable_range': 'claim_start_date out of reasonable range',
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
        'pharmacy_claim__dispensing_date_out_of_reasonable_range': 'dispensing_date out of reasonable range',
        'pharmacy_claim__dispensing_provider_npi_invalid': 'dispensing_provider_npi invalid',
        'pharmacy_claim__dispensing_provider_npi_null': 'dispensing_provider_npi null',
        'pharmacy_claim__ndc_code_invalid': 'ndc_code invalid',
        'pharmacy_claim__ndc_code_null': 'ndc_code null',
        'pharmacy_claim__no_matching_eligibility_span': 'no matching eligibility span',
        'pharmacy_claim__paid_amount_null': 'paid_amount null',
        'pharmacy_claim__paid_amount_gt_allowed_amount': 'paid_amount greater than allowed_amount',
        'pharmacy_claim__paid_amount_lt_zero': 'paid_amount less than zero',
        'pharmacy_claim__paid_date_null': 'paid_date null',
        'pharmacy_claim__paid_date_out_of_reasonable_range': 'paid_date out of reasonable range',
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
        'encounter__encounter_start_date_after_encounter_end_date': 'encounter_start_date after encounter_end_date',
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
        'lab_result__source_component_type_null_when_source_component_code_present': 'source_component_type null when source_component_code present',
        'lab_result__source_component_type_invalid': 'source_component_type invalid',
        'lab_result__source_component_code_invalid': 'source_component_code invalid',
        'location__npi_invalid': 'NPI invalid',
        'location__state_invalid': 'state invalid',
        'location__zip_code_invalid_format': 'zip_code invalid format',
        'medication__person_id_null': 'person_id null',
        'medication__patient_id_null': 'patient_id null',
        'medication__person_id_not_in_patient': 'person_id not found in patient',
        'medication__patient_id_not_in_patient': 'patient_id not found in patient',
        'medication__encounter_id_not_in_encounter': 'encounter_id not found in encounter',
        'medication__practitioner_id_not_in_practitioner': 'practitioner_id not found in practitioner',
        'medication__dispensing_date_out_of_range': 'dispensing_date out of range',
        'medication__prescribing_date_out_of_range': 'prescribing_date out of range',
        'medication__prescribing_date_after_dispensing_date': 'prescribing_date after dispensing_date',
        'medication__source_code_type_null_when_source_code_present': 'source_code_type null when source_code present',
        'medication__source_code_type_invalid': 'source_code_type invalid',
        'medication__source_code_null': 'source_code null',
        'medication__source_code_invalid': 'source_code invalid',
        'medication__ndc_code_invalid': 'ndc_code invalid',
        'medication__rxnorm_code_invalid': 'rxnorm_code invalid',
        'medication__atc_code_invalid': 'atc_code invalid',
        'medication__quantity_negative': 'quantity negative',
        'medication__days_supply_negative': 'days_supply negative',
        'observation__person_id_null': 'person_id null',
        'observation__patient_id_null': 'patient_id null',
        'observation__person_id_not_in_patient': 'person_id not found in patient',
        'observation__patient_id_not_in_patient': 'patient_id not found in patient',
        'observation__encounter_id_not_in_encounter': 'encounter_id not found in encounter',
        'observation__observation_date_null': 'observation_date null',
        'observation__observation_date_out_of_range': 'observation_date out of range',
        'observation__observation_type_invalid': 'observation_type invalid',
        'observation__source_code_type_null_when_source_code_present': 'source_code_type null when source_code present',
        'observation__source_code_type_invalid': 'source_code_type invalid',
        'observation__source_code_null': 'source_code null',
        'observation__source_code_invalid': 'source_code invalid',
        'patient__sex_null': 'sex null',
        'patient__sex_invalid': 'sex invalid',
        'patient__race_invalid': 'race invalid',
        'patient__ethnicity_invalid': 'ethnicity invalid',
        'patient__birth_date_null': 'birth_date null',
        'patient__birth_date_out_of_range': 'birth_date out of range',
        'patient__death_date_out_of_range': 'death_date out of range',
        'patient__birth_date_after_death_date': 'birth_date after death_date',
        'patient__death_flag_invalid': 'death_flag invalid',
        'patient__death_flag_without_death_date': 'death_flag indicates death without death_date',
        'patient__death_date_without_death_flag': 'death_date populated without death_flag',
        'patient__state_invalid': 'state invalid',
        'patient__zip_code_invalid_format': 'zip_code invalid format',
        'patient__multiple_sexes_per_person': 'sex has multiple values per person_id',
        'patient__multiple_birth_dates_per_person': 'birth_date has multiple values per person_id',
        'practitioner__npi_invalid': 'NPI invalid',
        'practitioner__npi_not_individual': 'NPI not individual',
        'procedure__person_id_null': 'person_id null',
        'procedure__patient_id_null': 'patient_id null',
        'procedure__person_id_not_in_patient': 'person_id not found in patient',
        'procedure__patient_id_not_in_patient': 'patient_id not found in patient',
        'procedure__encounter_id_not_in_encounter': 'encounter_id not found in encounter',
        'procedure__practitioner_id_not_in_practitioner': 'practitioner_id not found in practitioner',
        'procedure__procedure_date_null': 'procedure_date null',
        'procedure__procedure_date_out_of_range': 'procedure_date out of range',
        'procedure__code_system_null': 'code_system null',
        'procedure__code_system_invalid': 'code_system invalid',
        'procedure__source_code_null': 'source_code null',
        'procedure__source_code_invalid': 'source_code invalid'
    } %}

    {% set display_name = display_names.get(test_name, test_name) %}
    {% set display_name = display_name
        | replace(' null when ', ' is null when ')
        | replace(' null for ', ' is null for ')
        | replace(' null on ', ' is null on ')
        | replace(' null', ' is null')
        | replace(' invalid format', ' has invalid format')
        | replace(' invalid', ' is invalid')
        | replace(' out of reasonable range', ' is out of reasonable range')
        | replace(' out of range', ' is out of range')
        | replace(' less than zero', ' is less than zero')
        | replace(' greater than ', ' is greater than ')
        | replace(' after ', ' is after ')
        | replace(' indicates', ' indicates')
    %}
    {% set display_name = display_name
        | replace('is is ', 'is ')
        | replace('when source_code present', 'when source_code is present')
        | replace('when diagnosis_code present', 'when diagnosis_code is present')
        | replace('when procedure_code present', 'when procedure_code is present')
        | replace('when drg_code present', 'when drg_code is present')
        | replace('source_code is is present', 'source_code is present')
        | replace('diagnosis_code is is present', 'diagnosis_code is present')
        | replace('procedure_code is is present', 'procedure_code is present')
        | replace('drg_code is is present', 'drg_code is present')
    %}

    {% if ' not found in ' in display_name %}
        {% set display_name = display_name ~ ' table' %}
    {% endif %}

    {{ return(display_name) }}
{% endmacro %}

{% macro dq_logical_test_description(table_name, test_name) %}
    {% set display_name = dq_logical_display_name(table_name, test_name) %}
    {% set table_label = "input_layer." ~ table_name %}
    {% set descriptions = {
        'eligibility__birth_date_after_death_date': 'Checks whether birth_date is after death_date in input_layer.eligibility.',
        'eligibility__multiple_birth_dates_per_person': 'Checks whether the same person_id and data_source has more than one non-null birth_date in input_layer.eligibility.',
        'eligibility__birth_date_null': 'Checks whether birth_date is null in input_layer.eligibility.',
        'eligibility__birth_date_out_of_reasonable_range': 'Checks whether birth_date in input_layer.eligibility is before 1900-01-01 or after the current date.',
        'eligibility__death_flag_invalid': 'Checks whether death_flag in input_layer.eligibility is populated with a value other than 0 or 1.',
        'eligibility__death_flag_without_death_date': 'Checks whether death_flag is 1 in input_layer.eligibility while death_date is null.',
        'eligibility__death_date_out_of_reasonable_range': 'Checks whether death_date in input_layer.eligibility is before 1900-01-01 or after the current date.',
        'eligibility__enrollment_start_after_end': 'Checks whether enrollment_start_date is after enrollment_end_date in input_layer.eligibility.',
        'eligibility__sex_invalid': 'Checks whether sex in input_layer.eligibility is populated with a value other than male, female, or unknown.',
        'eligibility__multiple_sexes_per_person': 'Checks whether the same person_id and data_source has more than one non-null sex value in input_layer.eligibility.',
        'eligibility__sex_null': 'Checks whether sex is null in input_layer.eligibility.',
        'eligibility__payer_type_invalid': 'Checks whether payer_type in input_layer.eligibility is populated but not found in Tuva payer type terminology.',
        'eligibility__payer_type_null': 'Checks whether payer_type is null in input_layer.eligibility.',
        'eligibility__race_invalid': 'Checks whether race in input_layer.eligibility is populated but not found in Tuva race terminology.',
        'eligibility__multiple_races_per_person': 'Checks whether the same person_id and data_source has more than one non-null race value in input_layer.eligibility.',
        'eligibility__race_null': 'Checks whether race is null in input_layer.eligibility.',
        'medical_claim__admission_date_after_discharge_date': 'Checks whether admission_date is after discharge_date on a medical claim line.',
        'medical_claim__admission_date_has_multiple_values_per_inpatient_claim': 'Checks whether an inpatient facility claim_id has more than one non-null admission_date across claim lines.',
        'medical_claim__admission_date_out_of_reasonable_range': 'Checks whether admission_date on a medical claim line is before 2000-01-01 or after the current date.',
        'medical_claim__admit_source_code_invalid': 'Checks whether admit_source_code is populated but not found in Tuva admit source terminology.',
        'medical_claim__admit_type_code_invalid': 'Checks whether admit_type_code is populated but not found in Tuva admit type terminology.',
        'medical_claim__allowed_amount_null': 'Checks whether allowed_amount is null on a medical claim line.',
        'medical_claim__allowed_amount_lt_zero': 'Checks whether allowed_amount is less than zero on a medical claim line.',
        'medical_claim__bill_type_code_count_ne_one_for_institutional_claim': 'Checks whether an institutional claim_id has zero or more than one bill_type_code across claim lines.',
        'medical_claim__bill_type_code_invalid': 'Checks whether bill_type_code is populated but not found in Tuva bill type terminology.',
        'medical_claim__bill_type_code_null_for_institutional_claim': 'Checks whether bill_type_code is null on an institutional medical claim line.',
        'medical_claim__billing_npi_invalid': 'Checks whether billing_npi is populated but not found in Tuva provider data.',
        'medical_claim__billing_npi_null': 'Checks whether billing_npi is null on a medical claim line.',
        'medical_claim__claim_end_date_null': 'Checks whether claim_end_date is null on a medical claim line.',
        'medical_claim__claim_end_date_out_of_reasonable_range': 'Checks whether claim_end_date on a medical claim line is before 2000-01-01 or after the current date.',
        'medical_claim__claim_line_end_date_null': 'Checks whether claim_line_end_date is null on a medical claim line.',
        'medical_claim__claim_line_end_date_out_of_reasonable_range': 'Checks whether claim_line_end_date on a medical claim line is before 2000-01-01 or after the current date.',
        'medical_claim__claim_line_start_date_null': 'Checks whether claim_line_start_date is null on a medical claim line.',
        'medical_claim__claim_line_start_date_out_of_reasonable_range': 'Checks whether claim_line_start_date on a medical claim line is before 2000-01-01 or after the current date.',
        'medical_claim__claim_line_start_after_claim_line_end': 'Checks whether claim_line_start_date is after claim_line_end_date on a medical claim line.',
        'medical_claim__claim_start_after_claim_end': 'Checks whether claim_start_date is after claim_end_date on a medical claim line.',
        'medical_claim__claim_start_date_null': 'Checks whether claim_start_date is null on a medical claim line.',
        'medical_claim__claim_start_date_out_of_reasonable_range': 'Checks whether claim_start_date on a medical claim line is before 2000-01-01 or after the current date.',
        'medical_claim__claim_type_count_ne_one_per_claim': 'Checks whether a claim_id has zero or more than one claim_type across claim lines.',
        'medical_claim__claim_type_invalid': 'Checks whether claim_type is populated but not found in Tuva claim type terminology.',
        'medical_claim__claim_type_null': 'Checks whether claim_type is null on a medical claim line.',
        'medical_claim__institutional_indicators_present_for_professional_claim': 'Checks whether a professional claim line contains institutional-only fields such as bill type, revenue center, admit, discharge, or DRG fields.',
        'medical_claim__diagnosis_code_1_invalid': 'Checks whether diagnosis_code_1 is populated but not found in the ICD terminology indicated by diagnosis_code_type.',
        'medical_claim__diagnosis_code_1_null': 'Checks whether diagnosis_code_1 is null on a medical claim line.',
        'medical_claim__diagnosis_code_count_gt_one_per_position_for_institutional_claim': 'Checks whether an institutional claim_id has more than one distinct non-null diagnosis code in the same diagnosis position across claim lines.',
        'medical_claim__diagnosis_code_2_to_25_invalid': 'Checks whether diagnosis_code_2 through diagnosis_code_25 contain populated codes not found in the ICD terminology indicated by diagnosis_code_type.',
        'medical_claim__diagnosis_code_type_invalid': 'Checks whether diagnosis_code_type is populated with a value other than icd-9-cm or icd-10-cm.',
        'medical_claim__diagnosis_code_type_null_when_diagnosis_code_present': 'Checks whether diagnosis_code_type is null when any diagnosis code is populated on a medical claim line.',
        'medical_claim__discharge_disposition_code_invalid': 'Checks whether discharge_disposition_code is populated but not found in Tuva discharge disposition terminology.',
        'medical_claim__discharge_date_has_multiple_values_per_inpatient_claim': 'Checks whether an inpatient facility claim_id has more than one non-null discharge_date across claim lines.',
        'medical_claim__discharge_date_out_of_reasonable_range': 'Checks whether discharge_date on a medical claim line is before 2000-01-01 or after the current date.',
        'medical_claim__drg_code_count_ne_one_for_acute_inpatient_claim': 'Checks whether an acute inpatient claim_id has zero or more than one DRG code across claim lines.',
        'medical_claim__drg_code_invalid': 'Checks whether drg_code is populated but not found in the DRG terminology indicated by drg_code_type.',
        'medical_claim__drg_code_null_for_acute_inpatient_claim': 'Checks whether drg_code is null on an acute inpatient claim line.',
        'medical_claim__drg_code_type_invalid': 'Checks whether drg_code_type is populated with a value other than ms-drg or apr-drg.',
        'medical_claim__drg_code_type_null_when_drg_code_present': 'Checks whether drg_code_type is null when drg_code is populated on a medical claim line.',
        'medical_claim__admission_date_null_for_inpatient_claim': 'Checks whether admission_date is null on an inpatient facility claim line.',
        'medical_claim__discharge_date_null_for_inpatient_claim': 'Checks whether discharge_date is null on an inpatient facility claim line.',
        'medical_claim__facility_npi_invalid': 'Checks whether facility_npi is populated but not found in Tuva provider data.',
        'medical_claim__facility_npi_null_for_inpatient_claim': 'Checks whether facility_npi is null on an inpatient facility claim line.',
        'medical_claim__facility_npi_has_multiple_values_per_claim': 'Checks whether a claim_id has more than one non-null facility_npi across claim lines.',
        'medical_claim__hcpcs_code_null_for_professional_claim': 'Checks whether hcpcs_code is null on a professional claim line.',
        'medical_claim__no_matching_eligibility_span': 'Checks whether a medical claim line has no eligibility span for the same person_id, data_source, and claim dates.',
        'medical_claim__paid_amount_null': 'Checks whether paid_amount is null on a medical claim line.',
        'medical_claim__paid_amount_gt_allowed_amount': 'Checks whether paid_amount is greater than allowed_amount on a medical claim line.',
        'medical_claim__paid_amount_lt_zero': 'Checks whether paid_amount is less than zero on a medical claim line.',
        'medical_claim__multiple_person_ids_per_claim': 'Checks whether a claim_id is associated with more than one person_id within the same data_source.',
        'medical_claim__person_id_null': 'Checks whether person_id is null on a medical claim line.',
        'medical_claim__place_of_service_code_invalid': 'Checks whether place_of_service_code is populated but not found in Tuva place of service terminology.',
        'medical_claim__place_of_service_code_present_for_institutional_claim': 'Checks whether place_of_service_code is populated on an institutional claim line.',
        'medical_claim__place_of_service_code_null_for_professional_claim': 'Checks whether place_of_service_code is null on a professional claim line.',
        'medical_claim__procedure_code_1_to_25_invalid': 'Checks whether procedure_code_1 through procedure_code_25 contain populated codes not found in the ICD procedure terminology indicated by procedure_code_type.',
        'medical_claim__procedure_code_type_invalid': 'Checks whether procedure_code_type is populated with a value other than icd-9-pcs or icd-10-pcs.',
        'medical_claim__procedure_code_type_null_when_procedure_code_present': 'Checks whether procedure_code_type is null when any procedure code is populated on a medical claim line.',
        'medical_claim__rendering_npi_invalid': 'Checks whether rendering_npi is populated but not found in Tuva provider data.',
        'medical_claim__rendering_npi_null': 'Checks whether rendering_npi is null on a medical claim line.',
        'medical_claim__revenue_center_code_invalid': 'Checks whether revenue_center_code is populated but not found in Tuva revenue center terminology.',
        'medical_claim__revenue_center_code_null_for_institutional_claim': 'Checks whether revenue_center_code is null on an institutional claim line.',
        'pharmacy_claim__allowed_amount_null': 'Checks whether allowed_amount is null on a pharmacy claim line.',
        'pharmacy_claim__allowed_amount_lt_zero': 'Checks whether allowed_amount is less than zero on a pharmacy claim line.',
        'pharmacy_claim__dispensing_date_null': 'Checks whether dispensing_date is null on a pharmacy claim line.',
        'pharmacy_claim__dispensing_date_out_of_reasonable_range': 'Checks whether dispensing_date on a pharmacy claim line is before 2000-01-01 or after the current date.',
        'pharmacy_claim__dispensing_provider_npi_invalid': 'Checks whether dispensing_provider_npi is populated but not found in Tuva provider data.',
        'pharmacy_claim__dispensing_provider_npi_null': 'Checks whether dispensing_provider_npi is null on a pharmacy claim line.',
        'pharmacy_claim__ndc_code_invalid': 'Checks whether ndc_code is populated but not found in Tuva NDC terminology.',
        'pharmacy_claim__ndc_code_null': 'Checks whether ndc_code is null on a pharmacy claim line.',
        'pharmacy_claim__no_matching_eligibility_span': 'Checks whether a pharmacy claim has no eligibility span for the same person_id, data_source, and dispensing date.',
        'pharmacy_claim__paid_amount_null': 'Checks whether paid_amount is null on a pharmacy claim line.',
        'pharmacy_claim__paid_amount_gt_allowed_amount': 'Checks whether paid_amount is greater than allowed_amount on a pharmacy claim line.',
        'pharmacy_claim__paid_amount_lt_zero': 'Checks whether paid_amount is less than zero on a pharmacy claim line.',
        'pharmacy_claim__paid_date_null': 'Checks whether paid_date is null on a pharmacy claim line.',
        'pharmacy_claim__paid_date_out_of_reasonable_range': 'Checks whether paid_date on a pharmacy claim line is before 2000-01-01 or after the current date.',
        'pharmacy_claim__multiple_person_ids_per_claim': 'Checks whether a pharmacy claim_id is associated with more than one person_id within the same data_source.',
        'pharmacy_claim__person_id_null': 'Checks whether person_id is null on a pharmacy claim line.',
        'pharmacy_claim__prescribing_provider_npi_invalid': 'Checks whether prescribing_provider_npi is populated but not found in Tuva provider data.',
        'pharmacy_claim__prescribing_provider_npi_null': 'Checks whether prescribing_provider_npi is null on a pharmacy claim line.',
        'appointment__person_id_not_in_patient': 'Checks whether person_id values in input_layer.appointment have a corresponding person_id in input_layer.patient for the same data_source.',
        'appointment__patient_id_not_in_patient': 'Checks whether patient_id values in input_layer.appointment have a corresponding patient_id in input_layer.patient for the same data_source.',
        'appointment__encounter_id_not_in_encounter': 'Checks whether populated encounter_id values in input_layer.appointment have a corresponding encounter_id in input_layer.encounter for the same data_source.',
        'appointment__start_datetime_null': 'Checks whether start_datetime is null in input_layer.appointment.',
        'condition__person_id_null': 'Checks whether person_id is null in input_layer.condition.',
        'condition__patient_id_null': 'Checks whether patient_id is null in input_layer.condition.',
        'condition__source_code_null': 'Checks whether source_code is null in input_layer.condition.',
        'condition__code_system_null': 'Checks whether code_system is null in input_layer.condition.',
        'condition__person_id_not_in_patient': 'Checks whether person_id values in input_layer.condition have a corresponding person_id in input_layer.patient for the same data_source.',
        'condition__patient_id_not_in_patient': 'Checks whether patient_id values in input_layer.condition have a corresponding patient_id in input_layer.patient for the same data_source.',
        'condition__encounter_id_not_in_encounter': 'Checks whether populated encounter_id values in input_layer.condition have a corresponding encounter_id in input_layer.encounter for the same data_source.',
        'condition__code_system_invalid': 'Checks whether code_system in input_layer.condition is populated with a value other than icd-9-cm, icd-10-cm, snomed-ct, or unknown.',
        'condition__source_code_invalid': 'Checks whether source_code in input_layer.condition is populated for a supported standard code system but not found in the corresponding terminology table.',
        'condition__present_on_admit_code_invalid': 'Checks whether present_on_admit_code is populated but not found in Tuva present on admission terminology.',
        'encounter__person_id_null': 'Checks whether person_id is null in input_layer.encounter.',
        'encounter__patient_id_null': 'Checks whether patient_id is null in input_layer.encounter.',
        'encounter__person_id_not_in_patient': 'Checks whether person_id values in input_layer.encounter have a corresponding person_id in input_layer.patient for the same data_source.',
        'encounter__patient_id_not_in_patient': 'Checks whether patient_id values in input_layer.encounter have a corresponding patient_id in input_layer.patient for the same data_source.',
        'encounter__encounter_type_invalid': 'Checks whether encounter_type is populated but not found in Tuva encounter type terminology.',
        'encounter__encounter_start_date_null': 'Checks whether encounter_start_date is null in input_layer.encounter.',
        'encounter__encounter_end_date_null': 'Checks whether encounter_end_date is null in input_layer.encounter.',
        'encounter__encounter_start_date_after_encounter_end_date': 'Checks whether encounter_start_date is after encounter_end_date in input_layer.encounter.',
        'encounter__encounter_start_date_out_of_reasonable_range': 'Checks whether encounter_start_date in input_layer.encounter is before 2000-01-01 or after the current date.',
        'encounter__encounter_end_date_out_of_reasonable_range': 'Checks whether encounter_end_date in input_layer.encounter is before 2000-01-01 or after the current date.',
        'encounter__admit_source_code_invalid': 'Checks whether admit_source_code is populated but not found in Tuva admit source terminology.',
        'encounter__admit_type_code_invalid': 'Checks whether admit_type_code is populated but not found in Tuva admit type terminology.',
        'encounter__discharge_disposition_code_invalid': 'Checks whether discharge_disposition_code is populated but not found in Tuva discharge disposition terminology.',
        'encounter__facility_npi_invalid': 'Checks whether facility_npi is populated but not found in Tuva provider data.',
        'encounter__primary_diagnosis_code_type_null': 'Checks whether primary_diagnosis_code_type is null in input_layer.encounter.',
        'encounter__primary_diagnosis_code_type_invalid': 'Checks whether primary_diagnosis_code_type is populated with a value other than icd-9-cm or icd-10-cm.',
        'encounter__primary_diagnosis_code_null': 'Checks whether primary_diagnosis_code is null in input_layer.encounter.',
        'encounter__primary_diagnosis_code_invalid': 'Checks whether primary_diagnosis_code is populated but not found in the ICD terminology indicated by primary_diagnosis_code_type.',
        'encounter__drg_code_type_null': 'Checks whether drg_code_type is null in input_layer.encounter.',
        'encounter__drg_code_type_invalid': 'Checks whether drg_code_type is populated with a value other than ms-drg or apr-drg.',
        'encounter__drg_code_null': 'Checks whether drg_code is null in input_layer.encounter.',
        'encounter__drg_code_invalid': 'Checks whether drg_code is populated but not found in the DRG terminology indicated by drg_code_type.',
        'immunization__person_id_null': 'Checks whether person_id is null in input_layer.immunization.',
        'immunization__patient_id_null': 'Checks whether patient_id is null in input_layer.immunization.',
        'immunization__person_id_not_in_patient': 'Checks whether person_id values in input_layer.immunization have a corresponding person_id in input_layer.patient for the same data_source.',
        'immunization__patient_id_not_in_patient': 'Checks whether patient_id values in input_layer.immunization have a corresponding patient_id in input_layer.patient for the same data_source.',
        'immunization__encounter_id_not_in_encounter': 'Checks whether populated encounter_id values in input_layer.immunization have a corresponding encounter_id in input_layer.encounter for the same data_source.',
        'lab_result__person_id_null': 'Checks whether person_id is null in input_layer.lab_result.',
        'lab_result__patient_id_null': 'Checks whether patient_id is null in input_layer.lab_result.',
        'lab_result__person_id_not_in_patient': 'Checks whether person_id values in input_layer.lab_result have a corresponding person_id in input_layer.patient for the same data_source.',
        'lab_result__patient_id_not_in_patient': 'Checks whether patient_id values in input_layer.lab_result have a corresponding patient_id in input_layer.patient for the same data_source.',
        'lab_result__encounter_id_not_in_encounter': 'Checks whether populated encounter_id values in input_layer.lab_result have a corresponding encounter_id in input_layer.encounter for the same data_source.',
        'lab_result__accession_number_null': 'Checks whether accession_number is null in input_layer.lab_result.',
        'lab_result__source_component_type_null_when_source_component_code_present': 'Checks whether source_component_type is null when source_component_code is populated in input_layer.lab_result.',
        'lab_result__source_component_type_invalid': 'Checks whether source_component_type in input_layer.lab_result is populated with a value other than loinc, snomed-ct, local, or unknown.',
        'lab_result__source_component_code_invalid': 'Checks whether source_component_code in input_layer.lab_result is populated for LOINC or SNOMED CT but not found in the corresponding terminology table.',
        'location__npi_invalid': 'Checks whether npi is populated in input_layer.location but not found in Tuva provider data.',
        'location__state_invalid': 'Checks whether state is populated in input_layer.location but does not match an ANSI/FIPS state abbreviation, state name, or state code.',
        'location__zip_code_invalid_format': 'Checks whether zip_code is populated in input_layer.location but is not a 5-digit ZIP code, 9-digit ZIP code, or ZIP+4 value.',
        'medication__person_id_null': 'Checks whether person_id is null in input_layer.medication.',
        'medication__patient_id_null': 'Checks whether patient_id is null in input_layer.medication.',
        'medication__person_id_not_in_patient': 'Checks whether person_id values in input_layer.medication have a corresponding person_id in input_layer.patient for the same data_source.',
        'medication__patient_id_not_in_patient': 'Checks whether patient_id values in input_layer.medication have a corresponding patient_id in input_layer.patient for the same data_source.',
        'medication__encounter_id_not_in_encounter': 'Checks whether populated encounter_id values in input_layer.medication have a corresponding encounter_id in input_layer.encounter for the same data_source.',
        'medication__practitioner_id_not_in_practitioner': 'Checks whether populated practitioner_id values in input_layer.medication have a corresponding practitioner_id in input_layer.practitioner for the same data_source.',
        'medication__dispensing_date_out_of_range': 'Checks whether dispensing_date in input_layer.medication is before 2000-01-01 or after the current date.',
        'medication__prescribing_date_out_of_range': 'Checks whether prescribing_date in input_layer.medication is before 2000-01-01 or after the current date.',
        'medication__prescribing_date_after_dispensing_date': 'Checks whether prescribing_date is after dispensing_date in input_layer.medication.',
        'medication__source_code_type_null_when_source_code_present': 'Checks whether source_code_type is null when source_code is populated in input_layer.medication.',
        'medication__source_code_type_invalid': 'Checks whether source_code_type in input_layer.medication is populated with a value other than ndc, rxnorm, atc, local, or unknown.',
        'medication__source_code_null': 'Checks whether source_code is null when source_code_type is populated in input_layer.medication.',
        'medication__source_code_invalid': 'Checks whether source_code in input_layer.medication is populated for NDC, RxNorm, or ATC but not found in the corresponding terminology table.',
        'medication__ndc_code_invalid': 'Checks whether ndc_code is populated in input_layer.medication but not found in Tuva NDC terminology.',
        'medication__rxnorm_code_invalid': 'Checks whether rxnorm_code is populated in input_layer.medication but not found in Tuva RxNorm terminology.',
        'medication__atc_code_invalid': 'Checks whether atc_code is populated in input_layer.medication but not found in Tuva ATC terminology.',
        'medication__quantity_negative': 'Checks whether quantity is less than zero in input_layer.medication.',
        'medication__days_supply_negative': 'Checks whether days_supply is less than zero in input_layer.medication.',
        'observation__person_id_null': 'Checks whether person_id is null in input_layer.observation.',
        'observation__patient_id_null': 'Checks whether patient_id is null in input_layer.observation.',
        'observation__person_id_not_in_patient': 'Checks whether person_id values in input_layer.observation have a corresponding person_id in input_layer.patient for the same data_source.',
        'observation__patient_id_not_in_patient': 'Checks whether patient_id values in input_layer.observation have a corresponding patient_id in input_layer.patient for the same data_source.',
        'observation__encounter_id_not_in_encounter': 'Checks whether populated encounter_id values in input_layer.observation have a corresponding encounter_id in input_layer.encounter for the same data_source.',
        'observation__observation_date_null': 'Checks whether observation_date is null in input_layer.observation.',
        'observation__observation_date_out_of_range': 'Checks whether observation_date in input_layer.observation is before 2000-01-01 or after the current date.',
        'observation__observation_type_invalid': 'Checks whether observation_type is populated in input_layer.observation but not found in Tuva observation type terminology.',
        'observation__source_code_type_null_when_source_code_present': 'Checks whether source_code_type is null when source_code is populated in input_layer.observation.',
        'observation__source_code_type_invalid': 'Checks whether source_code_type in input_layer.observation is populated with a value other than loinc, snomed-ct, icd-10-cm, icd-9-cm, icd-10-pcs, icd-9-pcs, hcpcs, local, or unknown.',
        'observation__source_code_null': 'Checks whether source_code is null when source_code_type is populated in input_layer.observation.',
        'observation__source_code_invalid': 'Checks whether source_code in input_layer.observation is populated for a supported standard code system but not found in the corresponding terminology table.',
        'patient__sex_null': 'Checks whether sex is null in input_layer.patient.',
        'patient__sex_invalid': 'Checks whether sex in input_layer.patient is populated with a value other than male, female, or unknown.',
        'patient__race_invalid': 'Checks whether race in input_layer.patient is populated but not found in Tuva race terminology.',
        'patient__ethnicity_invalid': 'Checks whether ethnicity in input_layer.patient is populated but not found in Tuva ethnicity terminology.',
        'patient__birth_date_null': 'Checks whether birth_date is null in input_layer.patient.',
        'patient__birth_date_out_of_range': 'Checks whether birth_date in input_layer.patient is before 1900-01-01 or after the current date.',
        'patient__death_date_out_of_range': 'Checks whether death_date in input_layer.patient is before 1900-01-01 or after the current date.',
        'patient__birth_date_after_death_date': 'Checks whether birth_date is after death_date in input_layer.patient.',
        'patient__death_flag_invalid': 'Checks whether death_flag in input_layer.patient is populated with a value other than 0 or 1.',
        'patient__death_flag_without_death_date': 'Checks whether death_flag is 1 in input_layer.patient while death_date is null.',
        'patient__death_date_without_death_flag': 'Checks whether death_date is populated in input_layer.patient while death_flag is null or 0.',
        'patient__state_invalid': 'Checks whether state is populated in input_layer.patient but does not match an ANSI/FIPS state abbreviation, state name, or state code.',
        'patient__zip_code_invalid_format': 'Checks whether zip_code is populated in input_layer.patient but is not a 5-digit ZIP code, 9-digit ZIP code, or ZIP+4 value.',
        'patient__multiple_sexes_per_person': 'Checks whether the same person_id and data_source has more than one non-null sex value in input_layer.patient.',
        'patient__multiple_birth_dates_per_person': 'Checks whether the same person_id and data_source has more than one non-null birth_date in input_layer.patient.',
        'practitioner__npi_invalid': 'Checks whether npi is populated in input_layer.practitioner but not found in Tuva provider data.',
        'practitioner__npi_not_individual': 'Checks whether npi in input_layer.practitioner is found in Tuva provider data but has an NPPES entity type other than individual.',
        'procedure__person_id_null': 'Checks whether person_id is null in input_layer.procedure.',
        'procedure__patient_id_null': 'Checks whether patient_id is null in input_layer.procedure.',
        'procedure__person_id_not_in_patient': 'Checks whether person_id values in input_layer.procedure have a corresponding person_id in input_layer.patient for the same data_source.',
        'procedure__patient_id_not_in_patient': 'Checks whether patient_id values in input_layer.procedure have a corresponding patient_id in input_layer.patient for the same data_source.',
        'procedure__encounter_id_not_in_encounter': 'Checks whether populated encounter_id values in input_layer.procedure have a corresponding encounter_id in input_layer.encounter for the same data_source.',
        'procedure__practitioner_id_not_in_practitioner': 'Checks whether populated practitioner_id values in input_layer.procedure have a corresponding practitioner_id in input_layer.practitioner for the same data_source.',
        'procedure__procedure_date_null': 'Checks whether procedure_date is null in input_layer.procedure.',
        'procedure__procedure_date_out_of_range': 'Checks whether procedure_date in input_layer.procedure is before 2000-01-01 or after the current date.',
        'procedure__code_system_null': 'Checks whether code_system is null in input_layer.procedure.',
        'procedure__code_system_invalid': 'Checks whether code_system in input_layer.procedure is populated with a value other than icd-10-pcs, icd-9-pcs, hcpcs, snomed-ct, or unknown.',
        'procedure__source_code_null': 'Checks whether source_code is null in input_layer.procedure.',
        'procedure__source_code_invalid': 'Checks whether source_code in input_layer.procedure is populated for a supported standard code system but not found in the corresponding terminology table.'
    } %}

    {{ return(descriptions.get(test_name, "Checks whether " ~ display_name ~ " in " ~ table_label ~ ".")) }}
{% endmacro %}

{% macro dq_logical_test_type(test_name) %}
    {#
      Logical test types describe the kind of defect, independently from its
      downstream impact (severity). Keep these ordered from most specific to
      most general because some referential and consistency tests also contain
      words such as "null" or "invalid".
    #}
    {% set referential_test_patterns = [
        'not_in_',
        'no_matching_'
    ] %}
    {% set consistency_test_patterns = [
        'multiple_',
        '_has_multiple_values_',
        '_count_ne_one_',
        '_count_gt_one_',
        '_without_',
        'indicators_present_for_',
        '_present_for_'
    ] %}
    {% set missing_test_patterns = [
        '_null'
    ] %}
    {% set temporal_test_patterns = [
        '_out_of_range',
        '_out_of_reasonable_range',
        '_after_',
        '_start_after_',
        '_end_before_'
    ] %}
    {% set invalid_test_patterns = [
        '_invalid',
        '_invalid_format',
        '_negative',
        '_lt_zero',
        '_gt_allowed_amount',
        '_not_individual'
    ] %}

    {% for pattern in referential_test_patterns %}
        {% if pattern in test_name %}
            {{ return('referential') }}
        {% endif %}
    {% endfor %}
    {% for pattern in consistency_test_patterns %}
        {% if pattern in test_name %}
            {{ return('consistency') }}
        {% endif %}
    {% endfor %}
    {% for pattern in missing_test_patterns %}
        {% if pattern in test_name %}
            {{ return('missing') }}
        {% endif %}
    {% endfor %}
    {% for pattern in temporal_test_patterns %}
        {% if pattern in test_name %}
            {{ return('temporal') }}
        {% endif %}
    {% endfor %}
    {% for pattern in invalid_test_patterns %}
        {% if pattern in test_name %}
            {{ return('invalid') }}
        {% endif %}
    {% endfor %}

    {{ return('consistency') }}
{% endmacro %}

{% macro dq_logical_test_severity(test_name) %}
    {#
      Severity describes downstream impact, not defect frequency:
        1 = blocking keys, grain, or required routing fields
        2 = material analytic impact
        3 = secondary dimensional or enrichment impact
    #}
    {% set severity_1_patterns = [
        'person_id_null',
        'patient_id_null',
        'person_id_not_in_patient',
        'patient_id_not_in_patient',
        'claim_type_null',
        'claim_start_date_null',
        'claim_end_date_null',
        'claim_line_start_date_null',
        'claim_line_end_date_null',
        'dispensing_date_null',
        'enrollment_start_after_end',
        'multiple_person_ids_per_claim',
        'claim_type_count_ne_one_per_claim'
    ] %}
    {% set severity_3_patterns = [
        'sex_null',
        'sex_invalid',
        'race_null',
        'race_invalid',
        'ethnicity_invalid',
        'multiple_sexes_per_person',
        'multiple_races_per_person',
        'death_flag_invalid',
        'death_flag_without_death_date',
        'death_date_without_death_flag',
        'state_invalid',
        'zip_code_invalid_format',
        'paid_date_null',
        'accession_number_null',
        'practitioner_id_not_in_practitioner',
        'prescribing_provider_npi_null',
        'dispensing_provider_npi_null',
        'location__'
    ] %}

    {% for pattern in severity_1_patterns %}
        {% if pattern in test_name %}
            {{ return(1) }}
        {% endif %}
    {% endfor %}
    {% for pattern in severity_3_patterns %}
        {% if pattern in test_name %}
            {{ return(3) }}
        {% endif %}
    {% endfor %}

    {{ return(2) }}
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
