{% macro dq_logical_flag_column_name(test_name) %}
    {% set parts = test_name.split('__', 1) %}
    {% if parts | length == 2 %}
        {{ return(parts[1]) }}
    {% endif %}

    {{ return(test_name) }}
{% endmacro %}

{% macro dq_logical_int_flag_sql(failure_sql, applicability_sql) %}
    {{ return(
        "cast(case when " ~ applicability_sql ~ " then "
        ~ "case when " ~ failure_sql ~ " then 1 else 0 end "
        ~ "else null end as "
        ~ dbt.type_int() ~ ")"
    ) }}
{% endmacro %}

{% macro dq_logical_test_definitions() %}
    {# BEGIN LOGICAL TEST DEFINITIONS #}
    {% set test_definitions = [
        {
            "test_name": "eligibility__sex_null",
            "display_name": "sex is null",
            "description": "Checks whether sex is null in the eligibility Input Layer Model.",
            "test_type": "missing",
            "severity": 3,
            "affected_columns": ["sex"]
        },
        {
            "test_name": "eligibility__sex_invalid",
            "display_name": "sex is invalid",
            "description": "Checks whether sex in the eligibility Input Layer Model is populated with a value other than the exact lowercase values male, female, or unknown.",
            "test_type": "invalid",
            "severity": 3,
            "affected_columns": ["sex"]
        },
        {
            "test_name": "eligibility__race_null",
            "display_name": "race is null",
            "description": "Checks whether race is null in the eligibility Input Layer Model.",
            "test_type": "missing",
            "severity": 3,
            "affected_columns": ["race"]
        },
        {
            "test_name": "eligibility__race_invalid",
            "display_name": "race is invalid",
            "description": "Checks whether race in the eligibility Input Layer Model is populated but not found in Tuva race terminology.",
            "test_type": "invalid",
            "severity": 3,
            "affected_columns": ["race"]
        },
        {
            "test_name": "eligibility__birth_date_null",
            "display_name": "birth_date is null",
            "description": "Checks whether birth_date is null in the eligibility Input Layer Model.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["birth_date"]
        },
        {
            "test_name": "eligibility__birth_date_after_death_date",
            "display_name": "birth_date is after death_date",
            "description": "Checks whether birth_date is after death_date in the eligibility Input Layer Model.",
            "test_type": "temporal",
            "severity": 2,
            "affected_columns": ["birth_date", "death_date"]
        },
        {
            "test_name": "eligibility__birth_date_out_of_reasonable_range",
            "display_name": "birth_date is out of reasonable range",
            "description": "Checks whether birth_date in the eligibility Input Layer Model is before 1900-01-01 or after the current date.",
            "test_type": "temporal",
            "severity": 2,
            "affected_columns": ["birth_date"]
        },
        {
            "test_name": "eligibility__death_date_out_of_reasonable_range",
            "display_name": "death_date is out of reasonable range",
            "description": "Checks whether death_date in the eligibility Input Layer Model is before 1900-01-01 or after the current date.",
            "test_type": "temporal",
            "severity": 2,
            "affected_columns": ["death_date"]
        },
        {
            "test_name": "eligibility__birth_date_outside_supported_date_range",
            "display_name": "birth_date is outside the supported date range",
            "description": "Checks whether a populated birth_date in the eligibility Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["birth_date"]
        },
        {
            "test_name": "eligibility__death_date_outside_supported_date_range",
            "display_name": "death_date is outside the supported date range",
            "description": "Checks whether a populated death_date in the eligibility Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["death_date"]
        },
        {
            "test_name": "eligibility__enrollment_start_date_outside_supported_date_range",
            "display_name": "enrollment_start_date is outside the supported date range",
            "description": "Checks whether a populated enrollment_start_date in the eligibility Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["enrollment_start_date"]
        },
        {
            "test_name": "eligibility__enrollment_end_date_outside_supported_date_range",
            "display_name": "enrollment_end_date is outside the supported date range",
            "description": "Checks whether a populated finite enrollment_end_date in the eligibility Input Layer Model is before 1900-01-01 or after 2100-12-31. Null and the accepted 9999-12-31 open-span alias are not applicable. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["enrollment_end_date"]
        },
        {
            "test_name": "eligibility__file_date_outside_supported_date_range",
            "display_name": "file_date is outside the supported date range",
            "description": "Checks whether a populated file_date in the eligibility Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["file_date"]
        },
        {
            "test_name": "eligibility__death_flag_invalid",
            "display_name": "death flag is invalid",
            "description": "Checks whether death_flag in the eligibility Input Layer Model is populated with a value other than Boolean true or false. Warehouses may represent those Boolean values as 1 or 0.",
            "test_type": "invalid",
            "severity": 3,
            "affected_columns": ["death_flag"]
        },
        {
            "test_name": "eligibility__death_flag_without_death_date",
            "display_name": "death_flag indicates death without death_date",
            "description": "Checks whether death_flag is Boolean true in the eligibility Input Layer Model while death_date is null. Warehouses may represent true as 1.",
            "test_type": "consistency",
            "severity": 3,
            "affected_columns": ["death_date", "death_flag"]
        },
        {
            "test_name": "eligibility__enrollment_start_after_end",
            "display_name": "enrollment_start_date is after enrollment_end_date",
            "description": "Checks whether enrollment_start_date is after a populated finite enrollment_end_date in the eligibility Input Layer Model. Null and the legacy 9999-12-31 alias represent an open span and are not applicable.",
            "test_type": "temporal",
            "severity": 1,
            "affected_columns": ["enrollment_start_date", "enrollment_end_date"]
        },
        {
            "test_name": "eligibility__overlapping_enrollment_spans",
            "display_name": "overlapping enrollment spans",
            "description": "Checks whether an eligibility span overlaps another valid span for the same person_id, member_id, payer, plan, and data_source. Null and the legacy 9999-12-31 alias represent an end date of positive infinity. Adjacent non-overlapping finite spans pass.",
            "test_type": "temporal",
            "severity": 2,
            "affected_columns": ["person_id", "member_id", "enrollment_start_date", "enrollment_end_date", "payer", "plan", "data_source"]
        },
        {
            "test_name": "eligibility__multiple_open_enrollment_spans",
            "display_name": "multiple open enrollment spans",
            "description": "Checks whether more than one valid open eligibility span exists for the same person_id, member_id, payer, plan, and data_source. Null and the legacy 9999-12-31 alias both represent an open end date. Duplicate stable span identities remain a Structural Data Quality primary-key failure.",
            "test_type": "consistency",
            "severity": 2,
            "affected_columns": ["person_id", "member_id", "enrollment_start_date", "enrollment_end_date", "payer", "plan", "data_source"]
        },
        {
            "test_name": "eligibility__payer_type_null",
            "display_name": "payer_type is null",
            "description": "Checks whether payer_type is null in the eligibility Input Layer Model.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["payer_type"]
        },
        {
            "test_name": "eligibility__payer_type_invalid",
            "display_name": "payer_type is invalid",
            "description": "Checks whether payer_type in the eligibility Input Layer Model is populated but not found in Tuva payer type terminology.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["payer_type"]
        },
        {
            "test_name": "eligibility__multiple_sexes_per_person",
            "display_name": "sex has multiple values per person_id",
            "description": "Checks whether the same person_id and data_source identify records with more than one exact non-null sex value in the eligibility Input Layer Model. Letter-case variants are distinct values.",
            "test_type": "consistency",
            "severity": 3,
            "affected_columns": ["sex"]
        },
        {
            "test_name": "eligibility__multiple_races_per_person",
            "display_name": "race has multiple values per person_id",
            "description": "Checks whether the same person_id and data_source identify records with more than one non-null race value in the eligibility Input Layer Model.",
            "test_type": "consistency",
            "severity": 3,
            "affected_columns": ["race"]
        },
        {
            "test_name": "eligibility__multiple_birth_dates_per_person",
            "display_name": "birth_date has multiple values per person_id",
            "description": "Checks whether the same person_id and data_source identify records with more than one non-null birth_date in the eligibility Input Layer Model.",
            "test_type": "consistency",
            "severity": 2,
            "affected_columns": ["birth_date"]
        },
        {
            "test_name": "medical_claim__claim_type_null",
            "display_name": "claim_type is null",
            "description": "Checks whether claim_type is null on a medical claim line.",
            "test_type": "missing",
            "severity": 1,
            "affected_columns": ["claim_type"]
        },
        {
            "test_name": "medical_claim__claim_type_invalid",
            "display_name": "claim_type is invalid",
            "description": "Checks whether claim_type is populated with a value other than the exact lowercase values professional, institutional, or undetermined.",
            "test_type": "invalid",
            "severity": 1,
            "affected_columns": ["claim_type"]
        },
        {
            "test_name": "medical_claim__claim_line_number_not_positive",
            "display_name": "claim line number is not positive",
            "description": "Checks whether a populated claim_line_number on a medical claim is zero or negative.",
            "test_type": "invalid",
            "severity": 1,
            "affected_columns": ["claim_line_number"]
        },
        {
            "test_name": "medical_claim__institutional_indicators_present_for_professional_claim",
            "display_name": "institutional indicators present for professional claim",
            "description": "Checks whether a professional claim line contains a populated bill_type_code, drg_code, admit_type_code, admit_source_code, discharge_disposition_code, or revenue_center_code.",
            "test_type": "consistency",
            "severity": 2,
            "affected_columns": ["claim_type", "bill_type_code", "drg_code", "admit_type_code", "admit_source_code", "discharge_disposition_code", "revenue_center_code"]
        },
        {
            "test_name": "medical_claim__person_id_null",
            "display_name": "person_id is null",
            "description": "Checks whether person_id is null on a medical claim line.",
            "test_type": "missing",
            "severity": 1,
            "affected_columns": ["person_id"]
        },
        {
            "test_name": "medical_claim__claim_start_date_null",
            "display_name": "claim_start_date is null",
            "description": "Checks whether claim_start_date is null on a medical claim line.",
            "test_type": "missing",
            "severity": 1,
            "affected_columns": ["claim_start_date"]
        },
        {
            "test_name": "medical_claim__claim_end_date_null",
            "display_name": "claim_end_date is null",
            "description": "Checks whether claim_end_date is null on a medical claim line.",
            "test_type": "missing",
            "severity": 1,
            "affected_columns": ["claim_end_date"]
        },
        {
            "test_name": "medical_claim__claim_line_start_date_null",
            "display_name": "claim_line_start_date is null",
            "description": "Checks whether claim_line_start_date is null on a medical claim line.",
            "test_type": "missing",
            "severity": 1,
            "affected_columns": ["claim_line_start_date"]
        },
        {
            "test_name": "medical_claim__claim_line_end_date_null",
            "display_name": "claim_line_end_date is null",
            "description": "Checks whether claim_line_end_date is null on a medical claim line.",
            "test_type": "missing",
            "severity": 1,
            "affected_columns": ["claim_line_end_date"]
        },
        {
            "test_name": "medical_claim__claim_start_date_out_of_reasonable_range",
            "display_name": "claim_start_date is out of reasonable range",
            "description": "Checks whether claim_start_date on a medical claim line is before 2000-01-01 or after the current date.",
            "test_type": "temporal",
            "severity": 2,
            "affected_columns": ["claim_start_date"]
        },
        {
            "test_name": "medical_claim__claim_end_date_out_of_reasonable_range",
            "display_name": "claim_end_date is out of reasonable range",
            "description": "Checks whether claim_end_date on a medical claim line is before 2000-01-01 or after the current date.",
            "test_type": "temporal",
            "severity": 2,
            "affected_columns": ["claim_end_date"]
        },
        {
            "test_name": "medical_claim__claim_line_start_date_out_of_reasonable_range",
            "display_name": "claim_line_start_date is out of reasonable range",
            "description": "Checks whether claim_line_start_date on a medical claim line is before 2000-01-01 or after the current date.",
            "test_type": "temporal",
            "severity": 2,
            "affected_columns": ["claim_line_start_date"]
        },
        {
            "test_name": "medical_claim__claim_line_end_date_out_of_reasonable_range",
            "display_name": "claim_line_end_date is out of reasonable range",
            "description": "Checks whether claim_line_end_date on a medical claim line is before 2000-01-01 or after the current date.",
            "test_type": "temporal",
            "severity": 2,
            "affected_columns": ["claim_line_end_date"]
        },
        {
            "test_name": "medical_claim__claim_start_date_outside_supported_date_range",
            "display_name": "claim_start_date is outside the supported date range",
            "description": "Checks whether a populated claim_start_date in the medical_claim Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["claim_start_date"]
        },
        {
            "test_name": "medical_claim__claim_end_date_outside_supported_date_range",
            "display_name": "claim_end_date is outside the supported date range",
            "description": "Checks whether a populated claim_end_date in the medical_claim Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["claim_end_date"]
        },
        {
            "test_name": "medical_claim__claim_line_start_date_outside_supported_date_range",
            "display_name": "claim_line_start_date is outside the supported date range",
            "description": "Checks whether a populated claim_line_start_date in the medical_claim Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["claim_line_start_date"]
        },
        {
            "test_name": "medical_claim__claim_line_end_date_outside_supported_date_range",
            "display_name": "claim_line_end_date is outside the supported date range",
            "description": "Checks whether a populated claim_line_end_date in the medical_claim Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["claim_line_end_date"]
        },
        {
            "test_name": "medical_claim__claim_start_after_claim_end",
            "display_name": "claim_start_date is after claim_end_date",
            "description": "Checks whether claim_start_date is after claim_end_date on a medical claim line.",
            "test_type": "temporal",
            "severity": 2,
            "affected_columns": ["claim_start_date", "claim_end_date"]
        },
        {
            "test_name": "medical_claim__claim_line_start_after_claim_line_end",
            "display_name": "claim_line_start_date is after claim_line_end_date",
            "description": "Checks whether claim_line_start_date is after claim_line_end_date on a medical claim line.",
            "test_type": "temporal",
            "severity": 2,
            "affected_columns": ["claim_line_start_date", "claim_line_end_date"]
        },
        {
            "test_name": "medical_claim__admission_date_after_discharge_date",
            "display_name": "admission_date is after discharge_date",
            "description": "Checks whether admission_date is after discharge_date on a medical claim line.",
            "test_type": "temporal",
            "severity": 2,
            "affected_columns": ["admission_date", "discharge_date"]
        },
        {
            "test_name": "medical_claim__admission_date_out_of_reasonable_range",
            "display_name": "admission_date is out of reasonable range",
            "description": "Checks whether admission_date on a medical claim line is before 2000-01-01 or after the current date.",
            "test_type": "temporal",
            "severity": 2,
            "affected_columns": ["admission_date"]
        },
        {
            "test_name": "medical_claim__admission_date_outside_supported_date_range",
            "display_name": "admission_date is outside the supported date range",
            "description": "Checks whether a populated admission_date in the medical_claim Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["admission_date"]
        },
        {
            "test_name": "medical_claim__admission_date_null_for_inpatient_claim",
            "display_name": "admission_date is null for inpatient claim",
            "description": "Checks whether admission_date is null on an inpatient facility claim line.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["admission_date"]
        },
        {
            "test_name": "medical_claim__discharge_date_null_for_inpatient_claim",
            "display_name": "discharge_date is null for inpatient claim",
            "description": "Checks whether discharge_date is null on an inpatient facility claim line.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["discharge_date"]
        },
        {
            "test_name": "medical_claim__discharge_date_out_of_reasonable_range",
            "display_name": "discharge_date is out of reasonable range",
            "description": "Checks whether discharge_date on a medical claim line is before 2000-01-01 or after the current date.",
            "test_type": "temporal",
            "severity": 2,
            "affected_columns": ["discharge_date"]
        },
        {
            "test_name": "medical_claim__discharge_date_outside_supported_date_range",
            "display_name": "discharge_date is outside the supported date range",
            "description": "Checks whether a populated discharge_date in the medical_claim Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["discharge_date"]
        },
        {
            "test_name": "medical_claim__paid_amount_null",
            "display_name": "paid_amount is null",
            "description": "Checks whether paid_amount is null on a medical claim line.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["paid_amount"]
        },
        {
            "test_name": "medical_claim__paid_amount_lt_zero",
            "display_name": "paid_amount is less than zero",
            "description": "Checks whether paid_amount is less than zero on a medical claim line.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["paid_amount"]
        },
        {
            "test_name": "medical_claim__allowed_amount_null",
            "display_name": "allowed_amount is null",
            "description": "Checks whether allowed_amount is null on a medical claim line.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["allowed_amount"]
        },
        {
            "test_name": "medical_claim__allowed_amount_lt_zero",
            "display_name": "allowed_amount is less than zero",
            "description": "Checks whether allowed_amount is less than zero on a medical claim line.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["allowed_amount"]
        },
        {
            "test_name": "medical_claim__paid_amount_gt_allowed_amount",
            "display_name": "paid_amount is greater than allowed_amount",
            "description": "Checks whether paid_amount is greater than allowed_amount on a medical claim line.",
            "test_type": "consistency",
            "severity": 2,
            "affected_columns": ["allowed_amount", "paid_amount"]
        },
        {
            "test_name": "medical_claim__paid_date_null",
            "display_name": "paid date is null",
            "description": "Checks whether paid_date is null on a medical claim line.",
            "test_type": "missing",
            "severity": 3,
            "affected_columns": ["paid_date"]
        },
        {
            "test_name": "medical_claim__paid_date_out_of_reasonable_range",
            "display_name": "paid date is out of reasonable range",
            "description": "Checks whether a populated paid_date on a medical claim line is before 2000-01-01 or after the current date.",
            "test_type": "temporal",
            "severity": 2,
            "affected_columns": ["paid_date"]
        },
        {
            "test_name": "medical_claim__paid_date_outside_supported_date_range",
            "display_name": "paid_date is outside the supported date range",
            "description": "Checks whether a populated paid_date in the medical_claim Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["paid_date"]
        },
        {
            "test_name": "medical_claim__paid_date_before_claim_end_date",
            "display_name": "paid_date before claim_end_date",
            "description": "Checks whether a populated paid_date occurs before claim_end_date on a medical claim line.",
            "test_type": "temporal",
            "severity": 2,
            "affected_columns": ["paid_date", "claim_end_date"]
        },
        {
            "test_name": "medical_claim__procedure_date_1_outside_supported_date_range",
            "display_name": "procedure_date_1 is outside the supported date range",
            "description": "Checks whether a populated procedure_date_1 in the medical_claim Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["procedure_date_1"]
        },
        {
            "test_name": "medical_claim__procedure_date_2_outside_supported_date_range",
            "display_name": "procedure_date_2 is outside the supported date range",
            "description": "Checks whether a populated procedure_date_2 in the medical_claim Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["procedure_date_2"]
        },
        {
            "test_name": "medical_claim__procedure_date_3_outside_supported_date_range",
            "display_name": "procedure_date_3 is outside the supported date range",
            "description": "Checks whether a populated procedure_date_3 in the medical_claim Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["procedure_date_3"]
        },
        {
            "test_name": "medical_claim__procedure_date_4_outside_supported_date_range",
            "display_name": "procedure_date_4 is outside the supported date range",
            "description": "Checks whether a populated procedure_date_4 in the medical_claim Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["procedure_date_4"]
        },
        {
            "test_name": "medical_claim__procedure_date_5_outside_supported_date_range",
            "display_name": "procedure_date_5 is outside the supported date range",
            "description": "Checks whether a populated procedure_date_5 in the medical_claim Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["procedure_date_5"]
        },
        {
            "test_name": "medical_claim__procedure_date_6_outside_supported_date_range",
            "display_name": "procedure_date_6 is outside the supported date range",
            "description": "Checks whether a populated procedure_date_6 in the medical_claim Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["procedure_date_6"]
        },
        {
            "test_name": "medical_claim__procedure_date_7_outside_supported_date_range",
            "display_name": "procedure_date_7 is outside the supported date range",
            "description": "Checks whether a populated procedure_date_7 in the medical_claim Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["procedure_date_7"]
        },
        {
            "test_name": "medical_claim__procedure_date_8_outside_supported_date_range",
            "display_name": "procedure_date_8 is outside the supported date range",
            "description": "Checks whether a populated procedure_date_8 in the medical_claim Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["procedure_date_8"]
        },
        {
            "test_name": "medical_claim__procedure_date_9_outside_supported_date_range",
            "display_name": "procedure_date_9 is outside the supported date range",
            "description": "Checks whether a populated procedure_date_9 in the medical_claim Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["procedure_date_9"]
        },
        {
            "test_name": "medical_claim__procedure_date_10_outside_supported_date_range",
            "display_name": "procedure_date_10 is outside the supported date range",
            "description": "Checks whether a populated procedure_date_10 in the medical_claim Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["procedure_date_10"]
        },
        {
            "test_name": "medical_claim__procedure_date_11_outside_supported_date_range",
            "display_name": "procedure_date_11 is outside the supported date range",
            "description": "Checks whether a populated procedure_date_11 in the medical_claim Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["procedure_date_11"]
        },
        {
            "test_name": "medical_claim__procedure_date_12_outside_supported_date_range",
            "display_name": "procedure_date_12 is outside the supported date range",
            "description": "Checks whether a populated procedure_date_12 in the medical_claim Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["procedure_date_12"]
        },
        {
            "test_name": "medical_claim__procedure_date_13_outside_supported_date_range",
            "display_name": "procedure_date_13 is outside the supported date range",
            "description": "Checks whether a populated procedure_date_13 in the medical_claim Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["procedure_date_13"]
        },
        {
            "test_name": "medical_claim__procedure_date_14_outside_supported_date_range",
            "display_name": "procedure_date_14 is outside the supported date range",
            "description": "Checks whether a populated procedure_date_14 in the medical_claim Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["procedure_date_14"]
        },
        {
            "test_name": "medical_claim__procedure_date_15_outside_supported_date_range",
            "display_name": "procedure_date_15 is outside the supported date range",
            "description": "Checks whether a populated procedure_date_15 in the medical_claim Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["procedure_date_15"]
        },
        {
            "test_name": "medical_claim__procedure_date_16_outside_supported_date_range",
            "display_name": "procedure_date_16 is outside the supported date range",
            "description": "Checks whether a populated procedure_date_16 in the medical_claim Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["procedure_date_16"]
        },
        {
            "test_name": "medical_claim__procedure_date_17_outside_supported_date_range",
            "display_name": "procedure_date_17 is outside the supported date range",
            "description": "Checks whether a populated procedure_date_17 in the medical_claim Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["procedure_date_17"]
        },
        {
            "test_name": "medical_claim__procedure_date_18_outside_supported_date_range",
            "display_name": "procedure_date_18 is outside the supported date range",
            "description": "Checks whether a populated procedure_date_18 in the medical_claim Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["procedure_date_18"]
        },
        {
            "test_name": "medical_claim__procedure_date_19_outside_supported_date_range",
            "display_name": "procedure_date_19 is outside the supported date range",
            "description": "Checks whether a populated procedure_date_19 in the medical_claim Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["procedure_date_19"]
        },
        {
            "test_name": "medical_claim__procedure_date_20_outside_supported_date_range",
            "display_name": "procedure_date_20 is outside the supported date range",
            "description": "Checks whether a populated procedure_date_20 in the medical_claim Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["procedure_date_20"]
        },
        {
            "test_name": "medical_claim__procedure_date_21_outside_supported_date_range",
            "display_name": "procedure_date_21 is outside the supported date range",
            "description": "Checks whether a populated procedure_date_21 in the medical_claim Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["procedure_date_21"]
        },
        {
            "test_name": "medical_claim__procedure_date_22_outside_supported_date_range",
            "display_name": "procedure_date_22 is outside the supported date range",
            "description": "Checks whether a populated procedure_date_22 in the medical_claim Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["procedure_date_22"]
        },
        {
            "test_name": "medical_claim__procedure_date_23_outside_supported_date_range",
            "display_name": "procedure_date_23 is outside the supported date range",
            "description": "Checks whether a populated procedure_date_23 in the medical_claim Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["procedure_date_23"]
        },
        {
            "test_name": "medical_claim__procedure_date_24_outside_supported_date_range",
            "display_name": "procedure_date_24 is outside the supported date range",
            "description": "Checks whether a populated procedure_date_24 in the medical_claim Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["procedure_date_24"]
        },
        {
            "test_name": "medical_claim__procedure_date_25_outside_supported_date_range",
            "display_name": "procedure_date_25 is outside the supported date range",
            "description": "Checks whether a populated procedure_date_25 in the medical_claim Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["procedure_date_25"]
        },
        {
            "test_name": "medical_claim__file_date_outside_supported_date_range",
            "display_name": "file_date is outside the supported date range",
            "description": "Checks whether a populated file_date in the medical_claim Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["file_date"]
        },
        {
            "test_name": "medical_claim__admit_source_code_invalid",
            "display_name": "admit_source_code is invalid",
            "description": "Checks whether admit_source_code is populated on an institutional medical claim line but not found in Tuva admit source terminology.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["admit_source_code"]
        },
        {
            "test_name": "medical_claim__admit_type_code_invalid",
            "display_name": "admit_type_code is invalid",
            "description": "Checks whether admit_type_code is populated on an institutional medical claim line but not found in Tuva admit type terminology.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["admit_type_code"]
        },
        {
            "test_name": "medical_claim__discharge_disposition_code_invalid",
            "display_name": "discharge_disposition_code is invalid",
            "description": "Checks whether discharge_disposition_code is populated on an institutional medical claim line but not found in Tuva discharge disposition terminology.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["discharge_disposition_code"]
        },
        {
            "test_name": "medical_claim__place_of_service_code_invalid",
            "display_name": "place_of_service_code is invalid",
            "description": "Checks whether place_of_service_code is populated on a professional medical claim line but not found in Tuva place of service terminology.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["place_of_service_code"]
        },
        {
            "test_name": "medical_claim__bill_type_code_invalid",
            "display_name": "bill_type_code is invalid",
            "description": "Checks whether bill_type_code is populated on an institutional medical claim line but not found in Tuva bill type terminology.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["bill_type_code"]
        },
        {
            "test_name": "medical_claim__revenue_center_code_invalid",
            "display_name": "revenue_center_code is invalid",
            "description": "Checks whether revenue_center_code is populated on an institutional medical claim line but not found in Tuva revenue center terminology.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["revenue_center_code"]
        },
        {
            "test_name": "medical_claim__place_of_service_code_null_for_professional_claim",
            "display_name": "place_of_service_code is null for professional claim",
            "description": "Checks whether place_of_service_code is null on a professional claim line.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["place_of_service_code"]
        },
        {
            "test_name": "medical_claim__place_of_service_code_present_for_institutional_claim",
            "display_name": "place_of_service_code present for institutional claim",
            "description": "Checks whether place_of_service_code is populated on an institutional claim line.",
            "test_type": "consistency",
            "severity": 2,
            "affected_columns": ["place_of_service_code"]
        },
        {
            "test_name": "medical_claim__bill_type_code_null_for_institutional_claim",
            "display_name": "bill_type_code is null for institutional claim",
            "description": "Checks whether bill_type_code is null on an institutional medical claim line.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["bill_type_code"]
        },
        {
            "test_name": "medical_claim__revenue_center_code_null_for_institutional_claim",
            "display_name": "revenue_center_code is null for institutional claim",
            "description": "Checks whether revenue_center_code is null on an institutional claim line.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["revenue_center_code"]
        },
        {
            "test_name": "medical_claim__hcpcs_code_null_for_professional_claim",
            "display_name": "hcpcs_code is null for professional claim",
            "description": "Checks whether hcpcs_code is null on a professional claim line.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["hcpcs_code"]
        },
        {
            "test_name": "medical_claim__rendering_npi_invalid",
            "display_name": "rendering_npi is invalid",
            "description": "Checks whether rendering_npi is populated but not found in Tuva provider data.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["rendering_npi"]
        },
        {
            "test_name": "medical_claim__billing_npi_invalid",
            "display_name": "billing_npi is invalid",
            "description": "Checks whether billing_npi is populated but not found in Tuva provider data.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["billing_npi"]
        },
        {
            "test_name": "medical_claim__facility_npi_invalid",
            "display_name": "facility_npi is invalid",
            "description": "Checks whether facility_npi is populated but not found in Tuva provider data.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["facility_npi"]
        },
        {
            "test_name": "medical_claim__rendering_npi_null",
            "display_name": "rendering_npi is null",
            "description": "Checks whether rendering_npi is null on a medical claim line.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["rendering_npi"]
        },
        {
            "test_name": "medical_claim__billing_npi_null",
            "display_name": "billing_npi is null",
            "description": "Checks whether billing_npi is null on a medical claim line.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["billing_npi"]
        },
        {
            "test_name": "medical_claim__facility_npi_null_for_inpatient_claim",
            "display_name": "facility_npi is null for inpatient claim",
            "description": "Checks whether facility_npi is null on an inpatient facility claim line.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["facility_npi"]
        },
        {
            "test_name": "medical_claim__drg_code_type_null_when_drg_code_present",
            "display_name": "drg_code_type is null when drg_code is present",
            "description": "Checks whether drg_code_type is null when drg_code is populated on a medical claim line.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["drg_code_type", "drg_code"]
        },
        {
            "test_name": "medical_claim__drg_code_type_invalid",
            "display_name": "drg_code_type is invalid",
            "description": "Checks whether drg_code_type is populated with a value other than the exact lowercase values ms-drg or apr-drg.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["drg_code_type"]
        },
        {
            "test_name": "medical_claim__drg_code_invalid",
            "display_name": "drg_code is invalid",
            "description": "Checks whether drg_code is populated on an institutional medical claim line, drg_code_type equals the exact lowercase value ms-drg or apr-drg, and the code is not found in the corresponding DRG terminology.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["drg_code_type", "drg_code"]
        },
        {
            "test_name": "medical_claim__drg_code_null_for_acute_inpatient_claim",
            "display_name": "drg_code is null for acute inpatient claim",
            "description": "Checks whether drg_code is null on an acute inpatient claim line.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["drg_code"]
        },
        {
            "test_name": "medical_claim__diagnosis_code_1_null",
            "display_name": "diagnosis_code_1 is null",
            "description": "Checks whether diagnosis_code_1 is null on a medical claim line.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["diagnosis_code_1"]
        },
        {
            "test_name": "medical_claim__diagnosis_code_type_null_when_diagnosis_code_present",
            "display_name": "diagnosis_code_type is null when diagnosis_code is present",
            "description": "Checks whether diagnosis_code_type is null when any diagnosis code is populated on a medical claim line.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["diagnosis_code_type", "diagnosis_code_1", "diagnosis_code_2", "diagnosis_code_3", "diagnosis_code_4", "diagnosis_code_5", "diagnosis_code_6", "diagnosis_code_7", "diagnosis_code_8", "diagnosis_code_9", "diagnosis_code_10", "diagnosis_code_11", "diagnosis_code_12", "diagnosis_code_13", "diagnosis_code_14", "diagnosis_code_15", "diagnosis_code_16", "diagnosis_code_17", "diagnosis_code_18", "diagnosis_code_19", "diagnosis_code_20", "diagnosis_code_21", "diagnosis_code_22", "diagnosis_code_23", "diagnosis_code_24", "diagnosis_code_25"]
        },
        {
            "test_name": "medical_claim__diagnosis_code_type_invalid",
            "display_name": "diagnosis_code_type is invalid",
            "description": "Checks whether diagnosis_code_type is populated with a value other than the exact lowercase values icd-9-cm or icd-10-cm.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["diagnosis_code_type"]
        },
        {
            "test_name": "medical_claim__diagnosis_code_1_invalid",
            "display_name": "diagnosis_code_1 is invalid",
            "description": "Checks whether diagnosis_code_1 is populated, diagnosis_code_type equals the exact lowercase value icd-9-cm or icd-10-cm, and the code is not found in the corresponding ICD terminology.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["diagnosis_code_type", "diagnosis_code_1"]
        },
        {
            "test_name": "medical_claim__diagnosis_code_2_to_25_invalid",
            "display_name": "diagnosis_code_2 to diagnosis_code_25 is invalid",
            "description": "Checks whether diagnosis_code_2 through diagnosis_code_25 contain populated codes, diagnosis_code_type equals the exact lowercase value icd-9-cm or icd-10-cm, and at least one code is not found in the corresponding ICD terminology.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["diagnosis_code_type", "diagnosis_code_2", "diagnosis_code_3", "diagnosis_code_4", "diagnosis_code_5", "diagnosis_code_6", "diagnosis_code_7", "diagnosis_code_8", "diagnosis_code_9", "diagnosis_code_10", "diagnosis_code_11", "diagnosis_code_12", "diagnosis_code_13", "diagnosis_code_14", "diagnosis_code_15", "diagnosis_code_16", "diagnosis_code_17", "diagnosis_code_18", "diagnosis_code_19", "diagnosis_code_20", "diagnosis_code_21", "diagnosis_code_22", "diagnosis_code_23", "diagnosis_code_24", "diagnosis_code_25"]
        },
        {
            "test_name": "medical_claim__procedure_code_type_null_when_procedure_code_present",
            "display_name": "procedure_code_type is null when procedure_code is present",
            "description": "Checks whether procedure_code_type is null when any procedure code is populated on a medical claim line.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["procedure_code_type", "procedure_code_1", "procedure_code_2", "procedure_code_3", "procedure_code_4", "procedure_code_5", "procedure_code_6", "procedure_code_7", "procedure_code_8", "procedure_code_9", "procedure_code_10", "procedure_code_11", "procedure_code_12", "procedure_code_13", "procedure_code_14", "procedure_code_15", "procedure_code_16", "procedure_code_17", "procedure_code_18", "procedure_code_19", "procedure_code_20", "procedure_code_21", "procedure_code_22", "procedure_code_23", "procedure_code_24", "procedure_code_25"]
        },
        {
            "test_name": "medical_claim__procedure_code_type_invalid",
            "display_name": "procedure_code_type is invalid",
            "description": "Checks whether procedure_code_type is populated with a value other than the exact lowercase values icd-9-pcs or icd-10-pcs.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["procedure_code_type"]
        },
        {
            "test_name": "medical_claim__procedure_code_1_to_25_invalid",
            "display_name": "procedure_code_1 to procedure_code_25 is invalid",
            "description": "Checks whether procedure_code_1 through procedure_code_25 contain populated codes, procedure_code_type equals the exact lowercase value icd-9-pcs or icd-10-pcs, and at least one code is not found in the corresponding ICD procedure terminology.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["procedure_code_type", "procedure_code_1", "procedure_code_2", "procedure_code_3", "procedure_code_4", "procedure_code_5", "procedure_code_6", "procedure_code_7", "procedure_code_8", "procedure_code_9", "procedure_code_10", "procedure_code_11", "procedure_code_12", "procedure_code_13", "procedure_code_14", "procedure_code_15", "procedure_code_16", "procedure_code_17", "procedure_code_18", "procedure_code_19", "procedure_code_20", "procedure_code_21", "procedure_code_22", "procedure_code_23", "procedure_code_24", "procedure_code_25"]
        },
        {
            "test_name": "medical_claim__claim_type_count_ne_one_per_claim",
            "display_name": "claim_type has multiple values per claim_id",
            "description": "Checks whether a multi-line medical claim with at least one populated claim_type contains more than one distinct exact claim_type value, including values that differ only by letter case.",
            "test_type": "consistency",
            "severity": 1,
            "affected_columns": ["claim_type"]
        },
        {
            "test_name": "medical_claim__multiple_person_ids_per_claim",
            "display_name": "person_id has multiple values per claim",
            "description": "Checks whether a claim_id is associated with more than one person_id within the same data_source.",
            "test_type": "consistency",
            "severity": 1,
            "affected_columns": ["person_id"]
        },
        {
            "test_name": "medical_claim__facility_npi_has_multiple_values_per_claim",
            "display_name": "facility_npi has multiple values per claim_id",
            "description": "Checks whether a claim_id has more than one non-null facility_npi across claim lines.",
            "test_type": "consistency",
            "severity": 2,
            "affected_columns": ["facility_npi"]
        },
        {
            "test_name": "medical_claim__admission_date_has_multiple_values_per_inpatient_claim",
            "display_name": "admission_date has multiple values per inpatient claim",
            "description": "Checks whether an inpatient facility claim_id has more than one non-null admission_date across claim lines.",
            "test_type": "consistency",
            "severity": 2,
            "affected_columns": ["admission_date"]
        },
        {
            "test_name": "medical_claim__discharge_date_has_multiple_values_per_inpatient_claim",
            "display_name": "discharge_date has multiple values per inpatient claim",
            "description": "Checks whether an inpatient facility claim_id has more than one non-null discharge_date across claim lines.",
            "test_type": "consistency",
            "severity": 2,
            "affected_columns": ["discharge_date"]
        },
        {
            "test_name": "medical_claim__bill_type_code_count_ne_one_for_institutional_claim",
            "display_name": "bill_type_code has multiple values per institutional claim_id",
            "description": "Checks whether a multi-line institutional medical claim with at least one populated bill_type_code contains more than one distinct populated bill_type_code.",
            "test_type": "consistency",
            "severity": 2,
            "affected_columns": ["bill_type_code"]
        },
        {
            "test_name": "medical_claim__drg_code_count_ne_one_for_acute_inpatient_claim",
            "display_name": "drg_code has multiple values per acute inpatient claim_id",
            "description": "Checks whether a multi-line acute inpatient medical claim with at least one populated drg_code contains more than one distinct populated drg_code.",
            "test_type": "consistency",
            "severity": 2,
            "affected_columns": ["drg_code"]
        },
        {
            "test_name": "medical_claim__diagnosis_code_count_gt_one_per_position_for_institutional_claim",
            "display_name": "diagnosis_code has multiple values per position for institutional claim",
            "description": "Checks whether an institutional claim_id has more than one distinct non-null diagnosis code in the same diagnosis position across claim lines.",
            "test_type": "consistency",
            "severity": 2,
            "affected_columns": ["diagnosis_code_1", "diagnosis_code_2", "diagnosis_code_3", "diagnosis_code_4", "diagnosis_code_5", "diagnosis_code_6", "diagnosis_code_7", "diagnosis_code_8", "diagnosis_code_9", "diagnosis_code_10", "diagnosis_code_11", "diagnosis_code_12", "diagnosis_code_13", "diagnosis_code_14", "diagnosis_code_15", "diagnosis_code_16", "diagnosis_code_17", "diagnosis_code_18", "diagnosis_code_19", "diagnosis_code_20", "diagnosis_code_21", "diagnosis_code_22", "diagnosis_code_23", "diagnosis_code_24", "diagnosis_code_25"]
        },
        {
            "test_name": "medical_claim__member_id_has_multiple_values_per_claim",
            "display_name": "member id has multiple values per claim",
            "description": "Checks whether a multi-line medical claim has inconsistent member_id values, including a mix of null and non-null values.",
            "test_type": "consistency",
            "severity": 2,
            "affected_columns": ["member_id"]
        },
        {
            "test_name": "medical_claim__payer_has_multiple_values_per_claim",
            "display_name": "payer has multiple values per claim",
            "description": "Checks whether a multi-line medical claim has inconsistent payer values, including a mix of null and non-null values.",
            "test_type": "consistency",
            "severity": 2,
            "affected_columns": ["payer"]
        },
        {
            "test_name": "medical_claim__plan_has_multiple_values_per_claim",
            "display_name": "plan has multiple values per claim",
            "description": "Checks whether a multi-line medical claim has inconsistent plan values, including a mix of null and non-null values.",
            "test_type": "consistency",
            "severity": 2,
            "affected_columns": ["plan"]
        },
        {
            "test_name": "medical_claim__claim_start_date_has_multiple_values_per_claim",
            "display_name": "claim start date has multiple values per claim",
            "description": "Checks whether a multi-line medical claim has inconsistent claim_start_date values, including a mix of null and non-null values.",
            "test_type": "consistency",
            "severity": 1,
            "affected_columns": ["claim_start_date"]
        },
        {
            "test_name": "medical_claim__claim_end_date_has_multiple_values_per_claim",
            "display_name": "claim end date has multiple values per claim",
            "description": "Checks whether a multi-line medical claim has inconsistent claim_end_date values, including a mix of null and non-null values.",
            "test_type": "consistency",
            "severity": 1,
            "affected_columns": ["claim_end_date"]
        },
        {
            "test_name": "medical_claim__billing_npi_has_multiple_values_per_claim",
            "display_name": "billing npi has multiple values per claim",
            "description": "Checks whether a multi-line medical claim has inconsistent billing_npi values, including a mix of null and non-null values.",
            "test_type": "consistency",
            "severity": 2,
            "affected_columns": ["billing_npi"]
        },
        {
            "test_name": "medical_claim__admit_source_code_has_multiple_values_per_inpatient_claim",
            "display_name": "admit source code has multiple values per inpatient claim",
            "description": "Checks whether a multi-line inpatient medical claim has inconsistent admit_source_code values, including a mix of null and non-null values.",
            "test_type": "consistency",
            "severity": 2,
            "affected_columns": ["admit_source_code"]
        },
        {
            "test_name": "medical_claim__admit_type_code_has_multiple_values_per_inpatient_claim",
            "display_name": "admit type code has multiple values per inpatient claim",
            "description": "Checks whether a multi-line inpatient medical claim has inconsistent admit_type_code values, including a mix of null and non-null values.",
            "test_type": "consistency",
            "severity": 2,
            "affected_columns": ["admit_type_code"]
        },
        {
            "test_name": "medical_claim__discharge_disposition_code_has_multiple_values_per_inpatient_claim",
            "display_name": "discharge disposition code has multiple values per inpatient claim",
            "description": "Checks whether a multi-line inpatient medical claim has inconsistent discharge_disposition_code values, including a mix of null and non-null values.",
            "test_type": "consistency",
            "severity": 2,
            "affected_columns": ["discharge_disposition_code"]
        },
        {
            "test_name": "medical_claim__drg_code_type_has_multiple_values_per_acute_inpatient_claim",
            "display_name": "drg code type has multiple values per acute inpatient claim",
            "description": "Checks whether a multi-line acute inpatient medical claim has inconsistent exact drg_code_type values, including values that differ only by letter case or a mix of null and non-null values.",
            "test_type": "consistency",
            "severity": 2,
            "affected_columns": ["drg_code_type"]
        },
        {
            "test_name": "medical_claim__diagnosis_code_type_has_multiple_values_per_claim",
            "display_name": "diagnosis code type has multiple values per claim",
            "description": "Checks whether diagnosis-bearing lines in a multi-line medical claim have inconsistent exact diagnosis_code_type values, including values that differ only by letter case or a mix of null and non-null values.",
            "test_type": "consistency",
            "severity": 2,
            "affected_columns": ["diagnosis_code_type", "diagnosis_code_1", "diagnosis_code_2", "diagnosis_code_3", "diagnosis_code_4", "diagnosis_code_5", "diagnosis_code_6", "diagnosis_code_7", "diagnosis_code_8", "diagnosis_code_9", "diagnosis_code_10", "diagnosis_code_11", "diagnosis_code_12", "diagnosis_code_13", "diagnosis_code_14", "diagnosis_code_15", "diagnosis_code_16", "diagnosis_code_17", "diagnosis_code_18", "diagnosis_code_19", "diagnosis_code_20", "diagnosis_code_21", "diagnosis_code_22", "diagnosis_code_23", "diagnosis_code_24", "diagnosis_code_25"]
        },
        {
            "test_name": "medical_claim__procedure_code_type_has_multiple_values_per_claim",
            "display_name": "procedure code type has multiple values per claim",
            "description": "Checks whether procedure-bearing lines in a multi-line medical claim have inconsistent exact procedure_code_type values, including values that differ only by letter case or a mix of null and non-null values.",
            "test_type": "consistency",
            "severity": 2,
            "affected_columns": ["procedure_code_type", "procedure_code_1", "procedure_code_2", "procedure_code_3", "procedure_code_4", "procedure_code_5", "procedure_code_6", "procedure_code_7", "procedure_code_8", "procedure_code_9", "procedure_code_10", "procedure_code_11", "procedure_code_12", "procedure_code_13", "procedure_code_14", "procedure_code_15", "procedure_code_16", "procedure_code_17", "procedure_code_18", "procedure_code_19", "procedure_code_20", "procedure_code_21", "procedure_code_22", "procedure_code_23", "procedure_code_24", "procedure_code_25"]
        },
        {
            "test_name": "medical_claim__no_matching_eligibility_span",
            "display_name": "no matching eligibility span",
            "description": "Checks whether a medical claim line with a complete person_id, member_id, payer, plan, and data_source identity and a populated inferred claim date has no eligibility span for the same complete identity whose covered calendar months include the inferred claim date month. Only claim months on the supported member-month spine from 1900-01 through 2100-12 can match; populated dates outside that range fail. Eligibility coverage is evaluated only through the calendar month containing tuva_last_run: null and the legacy 9999-12-31 alias use that as-of boundary, and a later finite enrollment_end_date is capped to the same boundary. The inferred claim date uses claim_line_start_date, then claim_start_date, then admission_date.",
            "test_type": "referential",
            "severity": 2,
            "affected_columns": ["person_id", "member_id", "payer", "plan", "claim_line_start_date", "claim_start_date", "admission_date", "data_source"]
        },
        {
            "test_name": "pharmacy_claim__claim_line_number_not_positive",
            "display_name": "claim line number is not positive",
            "description": "Checks whether a populated claim_line_number on a pharmacy claim is zero or negative.",
            "test_type": "invalid",
            "severity": 1,
            "affected_columns": ["claim_line_number"]
        },
        {
            "test_name": "pharmacy_claim__person_id_null",
            "display_name": "person_id is null",
            "description": "Checks whether person_id is null on a pharmacy claim line.",
            "test_type": "missing",
            "severity": 1,
            "affected_columns": ["person_id"]
        },
        {
            "test_name": "pharmacy_claim__dispensing_date_null",
            "display_name": "dispensing_date is null",
            "description": "Checks whether dispensing_date is null on a pharmacy claim line.",
            "test_type": "missing",
            "severity": 1,
            "affected_columns": ["dispensing_date"]
        },
        {
            "test_name": "pharmacy_claim__paid_date_null",
            "display_name": "paid_date is null",
            "description": "Checks whether paid_date is null on a pharmacy claim line.",
            "test_type": "missing",
            "severity": 1,
            "affected_columns": ["paid_date"]
        },
        {
            "test_name": "pharmacy_claim__dispensing_date_out_of_reasonable_range",
            "display_name": "dispensing_date is out of reasonable range",
            "description": "Checks whether dispensing_date on a pharmacy claim line is before 2000-01-01 or after the current date.",
            "test_type": "temporal",
            "severity": 2,
            "affected_columns": ["dispensing_date"]
        },
        {
            "test_name": "pharmacy_claim__paid_date_out_of_reasonable_range",
            "display_name": "paid_date is out of reasonable range",
            "description": "Checks whether paid_date on a pharmacy claim line is before 2000-01-01 or after the current date.",
            "test_type": "temporal",
            "severity": 2,
            "affected_columns": ["paid_date"]
        },
        {
            "test_name": "pharmacy_claim__dispensing_date_outside_supported_date_range",
            "display_name": "dispensing_date is outside the supported date range",
            "description": "Checks whether a populated dispensing_date in the pharmacy_claim Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["dispensing_date"]
        },
        {
            "test_name": "pharmacy_claim__paid_date_outside_supported_date_range",
            "display_name": "paid_date is outside the supported date range",
            "description": "Checks whether a populated paid_date in the pharmacy_claim Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["paid_date"]
        },
        {
            "test_name": "pharmacy_claim__file_date_outside_supported_date_range",
            "display_name": "file_date is outside the supported date range",
            "description": "Checks whether a populated file_date in the pharmacy_claim Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["file_date"]
        },
        {
            "test_name": "pharmacy_claim__prescribing_provider_npi_null",
            "display_name": "prescribing_provider_npi is null",
            "description": "Checks whether prescribing_provider_npi is null on a pharmacy claim line.",
            "test_type": "missing",
            "severity": 3,
            "affected_columns": ["prescribing_provider_npi"]
        },
        {
            "test_name": "pharmacy_claim__prescribing_provider_npi_invalid",
            "display_name": "prescribing_provider_npi is invalid",
            "description": "Checks whether prescribing_provider_npi is populated but not found in Tuva provider data.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["prescribing_provider_npi"]
        },
        {
            "test_name": "pharmacy_claim__dispensing_provider_npi_null",
            "display_name": "dispensing_provider_npi is null",
            "description": "Checks whether dispensing_provider_npi is null on a pharmacy claim line.",
            "test_type": "missing",
            "severity": 3,
            "affected_columns": ["dispensing_provider_npi"]
        },
        {
            "test_name": "pharmacy_claim__dispensing_provider_npi_invalid",
            "display_name": "dispensing_provider_npi is invalid",
            "description": "Checks whether dispensing_provider_npi is populated but not found in Tuva provider data.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["dispensing_provider_npi"]
        },
        {
            "test_name": "pharmacy_claim__ndc_code_null",
            "display_name": "ndc_code is null",
            "description": "Checks whether ndc_code is null on a pharmacy claim line.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["ndc_code"]
        },
        {
            "test_name": "pharmacy_claim__ndc_code_invalid",
            "display_name": "ndc_code is invalid",
            "description": "Checks whether ndc_code is populated but not found in Tuva NDC terminology.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["ndc_code"]
        },
        {
            "test_name": "pharmacy_claim__quantity_null",
            "display_name": "quantity is null",
            "description": "Checks whether quantity is null on a pharmacy claim line.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["quantity"]
        },
        {
            "test_name": "pharmacy_claim__quantity_not_positive",
            "display_name": "quantity is not positive",
            "description": "Checks whether a populated quantity on a pharmacy claim line is zero or negative.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["quantity"]
        },
        {
            "test_name": "pharmacy_claim__days_supply_null",
            "display_name": "days supply is null",
            "description": "Checks whether days_supply is null on a pharmacy claim line.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["days_supply"]
        },
        {
            "test_name": "pharmacy_claim__days_supply_not_positive",
            "display_name": "days supply is not positive",
            "description": "Checks whether a populated days_supply on a pharmacy claim line is zero or negative.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["days_supply"]
        },
        {
            "test_name": "pharmacy_claim__refills_lt_zero",
            "display_name": "refills is less than zero",
            "description": "Checks whether a populated refills value on a pharmacy claim line is negative.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["refills"]
        },
        {
            "test_name": "pharmacy_claim__paid_amount_null",
            "display_name": "paid_amount is null",
            "description": "Checks whether paid_amount is null on a pharmacy claim line.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["paid_amount"]
        },
        {
            "test_name": "pharmacy_claim__paid_amount_lt_zero",
            "display_name": "paid_amount is less than zero",
            "description": "Checks whether paid_amount is less than zero on a pharmacy claim line.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["paid_amount"]
        },
        {
            "test_name": "pharmacy_claim__allowed_amount_null",
            "display_name": "allowed_amount is null",
            "description": "Checks whether allowed_amount is null on a pharmacy claim line.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["allowed_amount"]
        },
        {
            "test_name": "pharmacy_claim__allowed_amount_lt_zero",
            "display_name": "allowed_amount is less than zero",
            "description": "Checks whether allowed_amount is less than zero on a pharmacy claim line.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["allowed_amount"]
        },
        {
            "test_name": "pharmacy_claim__paid_amount_gt_allowed_amount",
            "display_name": "paid_amount is greater than allowed_amount",
            "description": "Checks whether paid_amount is greater than allowed_amount on a pharmacy claim line.",
            "test_type": "consistency",
            "severity": 2,
            "affected_columns": ["allowed_amount", "paid_amount"]
        },
        {
            "test_name": "pharmacy_claim__multiple_person_ids_per_claim",
            "display_name": "person_id has multiple values per claim",
            "description": "Checks whether a pharmacy claim_id is associated with more than one person_id within the same data_source.",
            "test_type": "consistency",
            "severity": 1,
            "affected_columns": ["person_id"]
        },
        {
            "test_name": "pharmacy_claim__no_matching_eligibility_span",
            "display_name": "no matching eligibility span",
            "description": "Checks whether a pharmacy claim line with a complete person_id, member_id, payer, plan, and data_source identity and populated paid_date has no eligibility span for the same complete identity whose covered calendar months include the paid_date month. Only paid months on the supported member-month spine from 1900-01 through 2100-12 can match; populated dates outside that range fail. Eligibility coverage is evaluated only through the calendar month containing tuva_last_run: null and the legacy 9999-12-31 alias use that as-of boundary, and a later finite enrollment_end_date is capped to the same boundary.",
            "test_type": "referential",
            "severity": 2,
            "affected_columns": ["person_id", "member_id", "payer", "plan", "paid_date", "data_source"]
        },
        {
            "test_name": "provider_attribution__year_month_invalid_format",
            "display_name": "year_month is invalid YYYYMM format",
            "description": "Checks whether a populated year_month in the provider_attribution Input Layer Model is an exact six-digit YYYYMM value with a valid month from 01 through 12.",
            "test_type": "invalid",
            "severity": 1,
            "affected_columns": ["year_month"]
        },
        {
            "test_name": "appointment__person_id_not_in_patient",
            "display_name": "person_id not found in patient table",
            "description": "Checks whether person_id values in the appointment Input Layer Model have a corresponding person_id in the patient Input Layer Model for the same data_source.",
            "test_type": "referential",
            "severity": 2,
            "affected_columns": ["person_id", "data_source"]
        },
        {
            "test_name": "appointment__patient_id_not_in_patient",
            "display_name": "patient_id not found in patient table",
            "description": "Checks whether patient_id values in the appointment Input Layer Model have a corresponding patient_id in the patient Input Layer Model for the same data_source.",
            "test_type": "referential",
            "severity": 2,
            "affected_columns": ["patient_id", "data_source"]
        },
        {
            "test_name": "appointment__person_patient_pair_not_in_patient",
            "display_name": "person_id and patient_id pair not found in patient table",
            "description": "Checks whether populated person_id and patient_id values each exist in the patient Input Layer Model for the same data_source but their exact pair does not.",
            "test_type": "referential",
            "severity": 2,
            "affected_columns": ["person_id", "patient_id", "data_source"]
        },
        {
            "test_name": "appointment__encounter_id_not_in_encounter",
            "display_name": "encounter_id not found in encounter table",
            "description": "Checks whether populated encounter_id values in the appointment Input Layer Model have a corresponding encounter_id in the encounter Input Layer Model for the same data_source.",
            "test_type": "referential",
            "severity": 2,
            "affected_columns": ["encounter_id", "data_source"]
        },
        {
            "test_name": "appointment__encounter_person_patient_pair_not_in_encounter",
            "display_name": "encounter_id, person_id, and patient_id combination not found in encounter table",
            "description": "Checks whether a populated encounter_id exists for the data_source and the populated person_id and patient_id pair exists in the patient Input Layer Model, but the encounter is not assigned to that exact pair in the encounter Input Layer Model.",
            "test_type": "referential",
            "severity": 2,
            "affected_columns": ["encounter_id", "person_id", "patient_id", "data_source"]
        },
        {
            "test_name": "appointment__start_datetime_null",
            "display_name": "start_datetime is null",
            "description": "Checks whether start_datetime is null in the appointment Input Layer Model.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["start_datetime"]
        },
        {
            "test_name": "appointment__end_datetime_before_start_datetime",
            "display_name": "end datetime before start datetime",
            "description": "Checks whether a populated appointment end_datetime occurs before start_datetime.",
            "test_type": "temporal",
            "severity": 2,
            "affected_columns": ["start_datetime", "end_datetime"]
        },
        {
            "test_name": "appointment__duration_negative",
            "display_name": "duration is negative",
            "description": "Checks whether a populated appointment duration is negative.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["duration"]
        },
        {
            "test_name": "appointment__type_code_invalid",
            "display_name": "type code is invalid",
            "description": "Checks whether a populated appointment type_code is absent from Tuva appointment-type terminology.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["type_code"]
        },
        {
            "test_name": "appointment__status_code_invalid",
            "display_name": "status code is invalid",
            "description": "Checks whether a populated appointment status_code is absent from Tuva appointment-status terminology.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["status_code"]
        },
        {
            "test_name": "condition__person_id_null",
            "display_name": "person_id is null",
            "description": "Checks whether person_id is null in the condition Input Layer Model.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["person_id"]
        },
        {
            "test_name": "condition__patient_id_null",
            "display_name": "patient_id is null",
            "description": "Checks whether patient_id is null in the condition Input Layer Model.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["patient_id"]
        },
        {
            "test_name": "condition__source_code_null",
            "display_name": "source_code is null",
            "description": "Checks whether source_code is null in the condition Input Layer Model.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["source_code"]
        },
        {
            "test_name": "condition__code_system_null",
            "display_name": "code_system is null",
            "description": "Checks whether code_system is null in the condition Input Layer Model.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["code_system"]
        },
        {
            "test_name": "condition__person_id_not_in_patient",
            "display_name": "person_id not found in patient table",
            "description": "Checks whether person_id values in the condition Input Layer Model have a corresponding person_id in the patient Input Layer Model for the same data_source.",
            "test_type": "referential",
            "severity": 2,
            "affected_columns": ["person_id", "data_source"]
        },
        {
            "test_name": "condition__patient_id_not_in_patient",
            "display_name": "patient_id not found in patient table",
            "description": "Checks whether patient_id values in the condition Input Layer Model have a corresponding patient_id in the patient Input Layer Model for the same data_source.",
            "test_type": "referential",
            "severity": 2,
            "affected_columns": ["patient_id", "data_source"]
        },
        {
            "test_name": "condition__person_patient_pair_not_in_patient",
            "display_name": "person_id and patient_id pair not found in patient table",
            "description": "Checks whether populated person_id and patient_id values each exist in the patient Input Layer Model for the same data_source but their exact pair does not.",
            "test_type": "referential",
            "severity": 2,
            "affected_columns": ["person_id", "patient_id", "data_source"]
        },
        {
            "test_name": "condition__encounter_id_not_in_encounter",
            "display_name": "encounter_id not found in encounter table",
            "description": "Checks whether populated encounter_id values in the condition Input Layer Model have a corresponding encounter_id in the encounter Input Layer Model for the same data_source.",
            "test_type": "referential",
            "severity": 2,
            "affected_columns": ["encounter_id", "data_source"]
        },
        {
            "test_name": "condition__encounter_person_patient_pair_not_in_encounter",
            "display_name": "encounter_id, person_id, and patient_id combination not found in encounter table",
            "description": "Checks whether a populated encounter_id exists for the data_source and the populated person_id and patient_id pair exists in the patient Input Layer Model, but the encounter is not assigned to that exact pair in the encounter Input Layer Model.",
            "test_type": "referential",
            "severity": 2,
            "affected_columns": ["encounter_id", "person_id", "patient_id", "data_source"]
        },
        {
            "test_name": "condition__code_system_invalid",
            "display_name": "code_system is invalid",
            "description": "Checks whether code_system in the condition Input Layer Model is populated with a value other than icd-9-cm, icd-10-cm, snomed-ct, or unknown.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["code_system"]
        },
        {
            "test_name": "condition__source_code_invalid",
            "display_name": "source_code is invalid",
            "description": "Checks whether source_code in the condition Input Layer Model is populated for a supported standard code system but not found in the corresponding terminology table.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["code_system", "source_code"]
        },
        {
            "test_name": "condition__present_on_admit_code_invalid",
            "display_name": "present_on_admit_code is invalid",
            "description": "Checks whether present_on_admit_code is populated but not found in Tuva present on admission terminology.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["present_on_admit_code"]
        },
        {
            "test_name": "condition__onset_date_after_resolved_date",
            "display_name": "onset date is after resolved date",
            "description": "Checks whether a populated condition onset_date occurs after resolved_date.",
            "test_type": "temporal",
            "severity": 2,
            "affected_columns": ["onset_date", "resolved_date"]
        },
        {
            "test_name": "condition__recorded_date_out_of_reasonable_range",
            "display_name": "recorded date is out of reasonable range",
            "description": "Checks whether a populated condition recorded_date is before 1900-01-01 or after the current date.",
            "test_type": "temporal",
            "severity": 2,
            "affected_columns": ["recorded_date"]
        },
        {
            "test_name": "condition__onset_date_out_of_reasonable_range",
            "display_name": "onset date is out of reasonable range",
            "description": "Checks whether a populated condition onset_date is before 1900-01-01 or after the current date.",
            "test_type": "temporal",
            "severity": 2,
            "affected_columns": ["onset_date"]
        },
        {
            "test_name": "condition__resolved_date_out_of_reasonable_range",
            "display_name": "resolved date is out of reasonable range",
            "description": "Checks whether a populated condition resolved_date is before 1900-01-01 or after the current date.",
            "test_type": "temporal",
            "severity": 2,
            "affected_columns": ["resolved_date"]
        },
        {
            "test_name": "condition__recorded_date_outside_supported_date_range",
            "display_name": "recorded_date is outside the supported date range",
            "description": "Checks whether a populated recorded_date in the condition Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["recorded_date"]
        },
        {
            "test_name": "condition__onset_date_outside_supported_date_range",
            "display_name": "onset_date is outside the supported date range",
            "description": "Checks whether a populated onset_date in the condition Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["onset_date"]
        },
        {
            "test_name": "condition__resolved_date_outside_supported_date_range",
            "display_name": "resolved_date is outside the supported date range",
            "description": "Checks whether a populated resolved_date in the condition Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["resolved_date"]
        },
        {
            "test_name": "encounter__person_id_null",
            "display_name": "person_id is null",
            "description": "Checks whether person_id is null in the encounter Input Layer Model.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["person_id"]
        },
        {
            "test_name": "encounter__patient_id_null",
            "display_name": "patient_id is null",
            "description": "Checks whether patient_id is null in the encounter Input Layer Model.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["patient_id"]
        },
        {
            "test_name": "encounter__person_id_not_in_patient",
            "display_name": "person_id not found in patient table",
            "description": "Checks whether person_id values in the encounter Input Layer Model have a corresponding person_id in the patient Input Layer Model for the same data_source.",
            "test_type": "referential",
            "severity": 2,
            "affected_columns": ["person_id", "data_source"]
        },
        {
            "test_name": "encounter__patient_id_not_in_patient",
            "display_name": "patient_id not found in patient table",
            "description": "Checks whether patient_id values in the encounter Input Layer Model have a corresponding patient_id in the patient Input Layer Model for the same data_source.",
            "test_type": "referential",
            "severity": 2,
            "affected_columns": ["patient_id", "data_source"]
        },
        {
            "test_name": "encounter__person_patient_pair_not_in_patient",
            "display_name": "person_id and patient_id pair not found in patient table",
            "description": "Checks whether populated person_id and patient_id values each exist in the patient Input Layer Model for the same data_source but their exact pair does not.",
            "test_type": "referential",
            "severity": 2,
            "affected_columns": ["person_id", "patient_id", "data_source"]
        },
        {
            "test_name": "encounter__encounter_type_invalid",
            "display_name": "encounter_type is invalid",
            "description": "Checks whether encounter_type is populated but does not exactly match a lowercase value in Tuva encounter type terminology.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["encounter_type"]
        },
        {
            "test_name": "encounter__encounter_start_date_null",
            "display_name": "encounter_start_date is null",
            "description": "Checks whether encounter_start_date is null in the encounter Input Layer Model.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["encounter_start_date"]
        },
        {
            "test_name": "encounter__encounter_end_date_null",
            "display_name": "encounter_end_date is null",
            "description": "Checks whether encounter_end_date is null in the encounter Input Layer Model.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["encounter_end_date"]
        },
        {
            "test_name": "encounter__encounter_start_date_after_encounter_end_date",
            "display_name": "encounter_start_date is after encounter_end_date",
            "description": "Checks whether encounter_start_date is after encounter_end_date in the encounter Input Layer Model.",
            "test_type": "temporal",
            "severity": 2,
            "affected_columns": ["encounter_start_date", "encounter_end_date"]
        },
        {
            "test_name": "encounter__encounter_start_date_out_of_reasonable_range",
            "display_name": "encounter_start_date is out of reasonable range",
            "description": "Checks whether encounter_start_date in the encounter Input Layer Model is before 2000-01-01 or after the current date.",
            "test_type": "temporal",
            "severity": 2,
            "affected_columns": ["encounter_start_date"]
        },
        {
            "test_name": "encounter__encounter_end_date_out_of_reasonable_range",
            "display_name": "encounter_end_date is out of reasonable range",
            "description": "Checks whether encounter_end_date in the encounter Input Layer Model is before 2000-01-01 or after the current date.",
            "test_type": "temporal",
            "severity": 2,
            "affected_columns": ["encounter_end_date"]
        },
        {
            "test_name": "encounter__encounter_start_date_outside_supported_date_range",
            "display_name": "encounter_start_date is outside the supported date range",
            "description": "Checks whether a populated encounter_start_date in the encounter Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["encounter_start_date"]
        },
        {
            "test_name": "encounter__encounter_end_date_outside_supported_date_range",
            "display_name": "encounter_end_date is outside the supported date range",
            "description": "Checks whether a populated encounter_end_date in the encounter Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["encounter_end_date"]
        },
        {
            "test_name": "encounter__admit_source_code_invalid",
            "display_name": "admit_source_code is invalid",
            "description": "Checks whether admit_source_code is populated but not found in Tuva admit source terminology.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["admit_source_code"]
        },
        {
            "test_name": "encounter__admit_type_code_invalid",
            "display_name": "admit_type_code is invalid",
            "description": "Checks whether admit_type_code is populated but not found in Tuva admit type terminology.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["admit_type_code"]
        },
        {
            "test_name": "encounter__discharge_disposition_code_invalid",
            "display_name": "discharge_disposition_code is invalid",
            "description": "Checks whether discharge_disposition_code is populated but not found in Tuva discharge disposition terminology.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["discharge_disposition_code"]
        },
        {
            "test_name": "encounter__facility_npi_invalid",
            "display_name": "facility_npi is invalid",
            "description": "Checks whether facility_npi is populated but not found in Tuva provider data.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["facility_npi"]
        },
        {
            "test_name": "encounter__primary_diagnosis_code_type_null",
            "display_name": "primary_diagnosis_code_type is null when primary_diagnosis_code is present",
            "description": "Checks whether primary_diagnosis_code_type is null when primary_diagnosis_code is populated in the encounter Input Layer Model.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["primary_diagnosis_code_type", "primary_diagnosis_code"]
        },
        {
            "test_name": "encounter__primary_diagnosis_code_type_invalid",
            "display_name": "primary_diagnosis_code_type is invalid",
            "description": "Checks whether primary_diagnosis_code_type is populated with a value other than the exact lowercase values icd-9-cm or icd-10-cm.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["primary_diagnosis_code_type"]
        },
        {
            "test_name": "encounter__primary_diagnosis_code_null",
            "display_name": "primary_diagnosis_code is null when primary_diagnosis_code_type is present",
            "description": "Checks whether primary_diagnosis_code is null when primary_diagnosis_code_type is populated in the encounter Input Layer Model.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["primary_diagnosis_code_type", "primary_diagnosis_code"]
        },
        {
            "test_name": "encounter__primary_diagnosis_code_invalid",
            "display_name": "primary_diagnosis_code is invalid",
            "description": "Checks whether primary_diagnosis_code is populated but not found in the ICD terminology indicated by primary_diagnosis_code_type.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["primary_diagnosis_code_type", "primary_diagnosis_code"]
        },
        {
            "test_name": "encounter__drg_code_type_null",
            "display_name": "drg_code_type is null when drg_code is present",
            "description": "Checks whether drg_code_type is null when drg_code is populated in the encounter Input Layer Model.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["drg_code_type", "drg_code"]
        },
        {
            "test_name": "encounter__drg_code_type_invalid",
            "display_name": "drg_code_type is invalid",
            "description": "Checks whether drg_code_type is populated with a value other than the exact lowercase values ms-drg or apr-drg.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["drg_code_type"]
        },
        {
            "test_name": "encounter__drg_code_null",
            "display_name": "drg_code is null when drg_code_type is present",
            "description": "Checks whether drg_code is null when drg_code_type is populated in the encounter Input Layer Model.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["drg_code_type", "drg_code"]
        },
        {
            "test_name": "encounter__drg_code_invalid",
            "display_name": "drg_code is invalid",
            "description": "Checks whether drg_code is populated but not found in the DRG terminology indicated by drg_code_type.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["drg_code_type", "drg_code"]
        },
        {
            "test_name": "immunization__person_id_null",
            "display_name": "person_id is null",
            "description": "Checks whether person_id is null in the immunization Input Layer Model.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["person_id"]
        },
        {
            "test_name": "immunization__patient_id_null",
            "display_name": "patient_id is null",
            "description": "Checks whether patient_id is null in the immunization Input Layer Model.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["patient_id"]
        },
        {
            "test_name": "immunization__person_id_not_in_patient",
            "display_name": "person_id not found in patient table",
            "description": "Checks whether person_id values in the immunization Input Layer Model have a corresponding person_id in the patient Input Layer Model for the same data_source.",
            "test_type": "referential",
            "severity": 2,
            "affected_columns": ["person_id", "data_source"]
        },
        {
            "test_name": "immunization__patient_id_not_in_patient",
            "display_name": "patient_id not found in patient table",
            "description": "Checks whether patient_id values in the immunization Input Layer Model have a corresponding patient_id in the patient Input Layer Model for the same data_source.",
            "test_type": "referential",
            "severity": 2,
            "affected_columns": ["patient_id", "data_source"]
        },
        {
            "test_name": "immunization__person_patient_pair_not_in_patient",
            "display_name": "person_id and patient_id pair not found in patient table",
            "description": "Checks whether populated person_id and patient_id values each exist in the patient Input Layer Model for the same data_source but their exact pair does not.",
            "test_type": "referential",
            "severity": 2,
            "affected_columns": ["person_id", "patient_id", "data_source"]
        },
        {
            "test_name": "immunization__encounter_id_not_in_encounter",
            "display_name": "encounter_id not found in encounter table",
            "description": "Checks whether populated encounter_id values in the immunization Input Layer Model have a corresponding encounter_id in the encounter Input Layer Model for the same data_source.",
            "test_type": "referential",
            "severity": 2,
            "affected_columns": ["encounter_id", "data_source"]
        },
        {
            "test_name": "immunization__encounter_person_patient_pair_not_in_encounter",
            "display_name": "encounter_id, person_id, and patient_id combination not found in encounter table",
            "description": "Checks whether a populated encounter_id exists for the data_source and the populated person_id and patient_id pair exists in the patient Input Layer Model, but the encounter is not assigned to that exact pair in the encounter Input Layer Model.",
            "test_type": "referential",
            "severity": 2,
            "affected_columns": ["encounter_id", "person_id", "patient_id", "data_source"]
        },
        {
            "test_name": "immunization__occurrence_date_null",
            "display_name": "occurrence date is null",
            "description": "Checks whether occurrence_date is null on an immunization record.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["occurrence_date"]
        },
        {
            "test_name": "immunization__occurrence_date_outside_supported_date_range",
            "display_name": "occurrence_date is outside the supported date range",
            "description": "Checks whether a populated occurrence_date in the immunization Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["occurrence_date"]
        },
        {
            "test_name": "immunization__source_code_type_null_when_source_code_present",
            "display_name": "source code type is null when source code present",
            "description": "Checks whether source_code_type is null when source_code is populated on an immunization record.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["source_code_type", "source_code"]
        },
        {
            "test_name": "immunization__source_code_null_when_source_code_type_present",
            "display_name": "source code is null when source code type present",
            "description": "Checks whether source_code is null when source_code_type is populated on an immunization record.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["source_code_type", "source_code"]
        },
        {
            "test_name": "immunization__source_code_invalid",
            "display_name": "source code is invalid",
            "description": "Checks whether a populated CVX source_code on an immunization record is absent from Tuva CVX terminology. Other code systems are not applicable.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["source_code_type", "source_code"]
        },
        {
            "test_name": "immunization__status_invalid",
            "display_name": "status is invalid",
            "description": "Checks whether a populated immunization status is absent from Tuva immunization-status terminology.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["status"]
        },
        {
            "test_name": "immunization__status_reason_invalid",
            "display_name": "status reason is invalid",
            "description": "Checks whether a populated immunization status_reason is absent from Tuva immunization-status-reason terminology.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["status_reason"]
        },
        {
            "test_name": "immunization__status_reason_present_for_completed_or_error_status",
            "display_name": "status reason present for completed or error status",
            "description": "Checks whether a status_reason is populated for a recognized immunization status other than not-done.",
            "test_type": "consistency",
            "severity": 2,
            "affected_columns": ["status", "status_reason"]
        },
        {
            "test_name": "lab_result__person_id_null",
            "display_name": "person_id is null",
            "description": "Checks whether person_id is null in the lab_result Input Layer Model.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["person_id"]
        },
        {
            "test_name": "lab_result__patient_id_null",
            "display_name": "patient_id is null",
            "description": "Checks whether patient_id is null in the lab_result Input Layer Model.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["patient_id"]
        },
        {
            "test_name": "lab_result__person_id_not_in_patient",
            "display_name": "person_id not found in patient table",
            "description": "Checks whether person_id values in the lab_result Input Layer Model have a corresponding person_id in the patient Input Layer Model for the same data_source.",
            "test_type": "referential",
            "severity": 2,
            "affected_columns": ["person_id", "data_source"]
        },
        {
            "test_name": "lab_result__patient_id_not_in_patient",
            "display_name": "patient_id not found in patient table",
            "description": "Checks whether patient_id values in the lab_result Input Layer Model have a corresponding patient_id in the patient Input Layer Model for the same data_source.",
            "test_type": "referential",
            "severity": 2,
            "affected_columns": ["patient_id", "data_source"]
        },
        {
            "test_name": "lab_result__person_patient_pair_not_in_patient",
            "display_name": "person_id and patient_id pair not found in patient table",
            "description": "Checks whether populated person_id and patient_id values each exist in the patient Input Layer Model for the same data_source but their exact pair does not.",
            "test_type": "referential",
            "severity": 2,
            "affected_columns": ["person_id", "patient_id", "data_source"]
        },
        {
            "test_name": "lab_result__encounter_id_not_in_encounter",
            "display_name": "encounter_id not found in encounter table",
            "description": "Checks whether populated encounter_id values in the lab_result Input Layer Model have a corresponding encounter_id in the encounter Input Layer Model for the same data_source.",
            "test_type": "referential",
            "severity": 2,
            "affected_columns": ["encounter_id", "data_source"]
        },
        {
            "test_name": "lab_result__encounter_person_patient_pair_not_in_encounter",
            "display_name": "encounter_id, person_id, and patient_id combination not found in encounter table",
            "description": "Checks whether a populated encounter_id exists for the data_source and the populated person_id and patient_id pair exists in the patient Input Layer Model, but the encounter is not assigned to that exact pair in the encounter Input Layer Model.",
            "test_type": "referential",
            "severity": 2,
            "affected_columns": ["encounter_id", "person_id", "patient_id", "data_source"]
        },
        {
            "test_name": "lab_result__accession_number_null",
            "display_name": "accession_number is null",
            "description": "Checks whether accession_number is null in the lab_result Input Layer Model.",
            "test_type": "missing",
            "severity": 3,
            "affected_columns": ["accession_number"]
        },
        {
            "test_name": "lab_result__source_component_type_null_when_source_component_code_present",
            "display_name": "source_component_type is null when source_component_code present",
            "description": "Checks whether source_component_type is null when source_component_code is populated in the lab_result Input Layer Model.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["source_component_type", "source_component_code"]
        },
        {
            "test_name": "lab_result__source_component_code_invalid",
            "display_name": "source_component_code is invalid",
            "description": "Checks whether a populated source_component_code is absent from the corresponding terminology when source_component_type identifies LOINC or SNOMED CT without regard to letter case. Every other source_component_type, including a source-system-specific name, is not applicable to this code-validity test.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["source_component_type", "source_component_code"]
        },
        {
            "test_name": "lab_result__source_order_type_null_when_source_order_code_present",
            "display_name": "source order type is null when source order code present",
            "description": "Checks whether source_order_type is null when source_order_code is populated on a lab result.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["source_order_type", "source_order_code"]
        },
        {
            "test_name": "lab_result__source_order_code_null_when_source_order_type_present",
            "display_name": "source order code is null when source order type present",
            "description": "Checks whether source_order_code is null when source_order_type is populated on a lab result.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["source_order_type", "source_order_code"]
        },
        {
            "test_name": "lab_result__source_order_code_invalid",
            "display_name": "source order code is invalid",
            "description": "Checks whether a populated source_order_code is absent from the corresponding terminology when source_order_type identifies LOINC or SNOMED CT without regard to letter case. Every other source_order_type, including a source-system-specific name, is not applicable to this code-validity test.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["source_order_type", "source_order_code"]
        },
        {
            "test_name": "lab_result__collection_datetime_after_result_datetime",
            "display_name": "collection datetime is after result datetime",
            "description": "Checks whether a populated collection_datetime occurs after result_datetime.",
            "test_type": "temporal",
            "severity": 2,
            "affected_columns": ["collection_datetime", "result_datetime"]
        },
        {
            "test_name": "lab_result__result_datetime_out_of_reasonable_range",
            "display_name": "result datetime is out of reasonable range",
            "description": "Checks whether a populated result_datetime is before 2000-01-01 or after the current date.",
            "test_type": "temporal",
            "severity": 2,
            "affected_columns": ["result_datetime"]
        },
        {
            "test_name": "lab_result__collection_datetime_out_of_reasonable_range",
            "display_name": "collection datetime is out of reasonable range",
            "description": "Checks whether a populated collection_datetime is before 2000-01-01 or after the current date.",
            "test_type": "temporal",
            "severity": 2,
            "affected_columns": ["collection_datetime"]
        },
        {
            "test_name": "location__npi_invalid",
            "display_name": "NPI is invalid",
            "description": "Checks whether npi is populated in the location Input Layer Model but not found in Tuva provider data.",
            "test_type": "invalid",
            "severity": 3,
            "affected_columns": ["npi"]
        },
        {
            "test_name": "medication__person_id_null",
            "display_name": "person_id is null",
            "description": "Checks whether person_id is null in the medication Input Layer Model.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["person_id"]
        },
        {
            "test_name": "medication__patient_id_null",
            "display_name": "patient_id is null",
            "description": "Checks whether patient_id is null in the medication Input Layer Model.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["patient_id"]
        },
        {
            "test_name": "medication__person_id_not_in_patient",
            "display_name": "person_id not found in patient table",
            "description": "Checks whether person_id values in the medication Input Layer Model have a corresponding person_id in the patient Input Layer Model for the same data_source.",
            "test_type": "referential",
            "severity": 2,
            "affected_columns": ["person_id", "data_source"]
        },
        {
            "test_name": "medication__patient_id_not_in_patient",
            "display_name": "patient_id not found in patient table",
            "description": "Checks whether patient_id values in the medication Input Layer Model have a corresponding patient_id in the patient Input Layer Model for the same data_source.",
            "test_type": "referential",
            "severity": 2,
            "affected_columns": ["patient_id", "data_source"]
        },
        {
            "test_name": "medication__person_patient_pair_not_in_patient",
            "display_name": "person_id and patient_id pair not found in patient table",
            "description": "Checks whether populated person_id and patient_id values each exist in the patient Input Layer Model for the same data_source but their exact pair does not.",
            "test_type": "referential",
            "severity": 2,
            "affected_columns": ["person_id", "patient_id", "data_source"]
        },
        {
            "test_name": "medication__encounter_id_not_in_encounter",
            "display_name": "encounter_id not found in encounter table",
            "description": "Checks whether populated encounter_id values in the medication Input Layer Model have a corresponding encounter_id in the encounter Input Layer Model for the same data_source.",
            "test_type": "referential",
            "severity": 2,
            "affected_columns": ["encounter_id", "data_source"]
        },
        {
            "test_name": "medication__encounter_person_patient_pair_not_in_encounter",
            "display_name": "encounter_id, person_id, and patient_id combination not found in encounter table",
            "description": "Checks whether a populated encounter_id exists for the data_source and the populated person_id and patient_id pair exists in the patient Input Layer Model, but the encounter is not assigned to that exact pair in the encounter Input Layer Model.",
            "test_type": "referential",
            "severity": 2,
            "affected_columns": ["encounter_id", "person_id", "patient_id", "data_source"]
        },
        {
            "test_name": "medication__practitioner_id_not_in_practitioner",
            "display_name": "practitioner_id not found in practitioner table",
            "description": "Checks whether populated practitioner_id values in the medication Input Layer Model have a corresponding practitioner_id in the practitioner Input Layer Model for the same data_source.",
            "test_type": "referential",
            "severity": 3,
            "affected_columns": ["practitioner_id", "data_source"]
        },
        {
            "test_name": "medication__dispensing_date_out_of_range",
            "display_name": "dispensing_date is out of range",
            "description": "Checks whether dispensing_date in the medication Input Layer Model is before 2000-01-01 or after the current date.",
            "test_type": "temporal",
            "severity": 2,
            "affected_columns": ["dispensing_date"]
        },
        {
            "test_name": "medication__prescribing_date_out_of_range",
            "display_name": "prescribing_date is out of range",
            "description": "Checks whether prescribing_date in the medication Input Layer Model is before 2000-01-01 or after the current date.",
            "test_type": "temporal",
            "severity": 2,
            "affected_columns": ["prescribing_date"]
        },
        {
            "test_name": "medication__dispensing_date_outside_supported_date_range",
            "display_name": "dispensing_date is outside the supported date range",
            "description": "Checks whether a populated dispensing_date in the medication Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["dispensing_date"]
        },
        {
            "test_name": "medication__prescribing_date_outside_supported_date_range",
            "display_name": "prescribing_date is outside the supported date range",
            "description": "Checks whether a populated prescribing_date in the medication Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["prescribing_date"]
        },
        {
            "test_name": "medication__prescribing_date_after_dispensing_date",
            "display_name": "prescribing_date is after dispensing_date",
            "description": "Checks whether prescribing_date is after dispensing_date in the medication Input Layer Model.",
            "test_type": "temporal",
            "severity": 2,
            "affected_columns": ["dispensing_date", "prescribing_date"]
        },
        {
            "test_name": "medication__source_code_type_null_when_source_code_present",
            "display_name": "source_code_type is null when source_code is present",
            "description": "Checks whether source_code_type is null when source_code is populated in the medication Input Layer Model.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["source_code_type", "source_code"]
        },
        {
            "test_name": "medication__source_code_null",
            "display_name": "source_code is null",
            "description": "Checks whether source_code is null when source_code_type is populated in the medication Input Layer Model.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["source_code_type", "source_code"]
        },
        {
            "test_name": "medication__source_code_invalid",
            "display_name": "source_code is invalid",
            "description": "Checks whether a populated source_code is absent from Tuva NDC terminology when source_code_type identifies NDC without regard to letter case. Tuva removes hyphens from both values before comparison. Every other source_code_type, including RxNorm, ATC, and a source-system-specific name, is not applicable to this code-validity test.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["source_code_type", "source_code"]
        },
        {
            "test_name": "medication__ndc_code_invalid",
            "display_name": "ndc_code is invalid",
            "description": "Checks whether a populated ndc_code is absent from Tuva NDC terminology after Tuva removes hyphens from both values before comparison.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["ndc_code"]
        },
        {
            "test_name": "medication__quantity_negative",
            "display_name": "quantity is negative",
            "description": "Checks whether quantity is less than zero in the medication Input Layer Model.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["quantity"]
        },
        {
            "test_name": "medication__days_supply_negative",
            "display_name": "days_supply is negative",
            "description": "Checks whether days_supply is less than zero in the medication Input Layer Model.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["days_supply"]
        },
        {
            "test_name": "observation__person_id_null",
            "display_name": "person_id is null",
            "description": "Checks whether person_id is null in the observation Input Layer Model.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["person_id"]
        },
        {
            "test_name": "observation__patient_id_null",
            "display_name": "patient_id is null",
            "description": "Checks whether patient_id is null in the observation Input Layer Model.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["patient_id"]
        },
        {
            "test_name": "observation__person_id_not_in_patient",
            "display_name": "person_id not found in patient table",
            "description": "Checks whether person_id values in the observation Input Layer Model have a corresponding person_id in the patient Input Layer Model for the same data_source.",
            "test_type": "referential",
            "severity": 2,
            "affected_columns": ["person_id", "data_source"]
        },
        {
            "test_name": "observation__patient_id_not_in_patient",
            "display_name": "patient_id not found in patient table",
            "description": "Checks whether patient_id values in the observation Input Layer Model have a corresponding patient_id in the patient Input Layer Model for the same data_source.",
            "test_type": "referential",
            "severity": 2,
            "affected_columns": ["patient_id", "data_source"]
        },
        {
            "test_name": "observation__person_patient_pair_not_in_patient",
            "display_name": "person_id and patient_id pair not found in patient table",
            "description": "Checks whether populated person_id and patient_id values each exist in the patient Input Layer Model for the same data_source but their exact pair does not.",
            "test_type": "referential",
            "severity": 2,
            "affected_columns": ["person_id", "patient_id", "data_source"]
        },
        {
            "test_name": "observation__encounter_id_not_in_encounter",
            "display_name": "encounter_id not found in encounter table",
            "description": "Checks whether populated encounter_id values in the observation Input Layer Model have a corresponding encounter_id in the encounter Input Layer Model for the same data_source.",
            "test_type": "referential",
            "severity": 2,
            "affected_columns": ["encounter_id", "data_source"]
        },
        {
            "test_name": "observation__encounter_person_patient_pair_not_in_encounter",
            "display_name": "encounter_id, person_id, and patient_id combination not found in encounter table",
            "description": "Checks whether a populated encounter_id exists for the data_source and the populated person_id and patient_id pair exists in the patient Input Layer Model, but the encounter is not assigned to that exact pair in the encounter Input Layer Model.",
            "test_type": "referential",
            "severity": 2,
            "affected_columns": ["encounter_id", "person_id", "patient_id", "data_source"]
        },
        {
            "test_name": "observation__observation_date_null",
            "display_name": "observation_date is null",
            "description": "Checks whether observation_date is null in the observation Input Layer Model.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["observation_date"]
        },
        {
            "test_name": "observation__observation_date_out_of_range",
            "display_name": "observation_date is out of range",
            "description": "Checks whether observation_date in the observation Input Layer Model is before 2000-01-01 or after the current date.",
            "test_type": "temporal",
            "severity": 2,
            "affected_columns": ["observation_date"]
        },
        {
            "test_name": "observation__observation_date_outside_supported_date_range",
            "display_name": "observation_date is outside the supported date range",
            "description": "Checks whether a populated observation_date in the observation Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["observation_date"]
        },
        {
            "test_name": "observation__observation_type_invalid",
            "display_name": "observation_type is invalid",
            "description": "Checks whether observation_type is populated in the observation Input Layer Model but not found in Tuva observation type terminology.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["observation_type"]
        },
        {
            "test_name": "observation__source_code_type_null_when_source_code_present",
            "display_name": "source_code_type is null when source_code is present",
            "description": "Checks whether source_code_type is null when source_code is populated in the observation Input Layer Model.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["source_code_type", "source_code"]
        },
        {
            "test_name": "observation__source_code_null",
            "display_name": "source_code is null",
            "description": "Checks whether source_code is null when source_code_type is populated in the observation Input Layer Model.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["source_code_type", "source_code"]
        },
        {
            "test_name": "observation__source_code_invalid",
            "display_name": "source_code is invalid",
            "description": "Checks whether a populated source_code is absent from the corresponding terminology when source_code_type identifies LOINC, SNOMED CT, ICD-10-CM, ICD-9-CM, ICD-10-PCS, or ICD-9-PCS without regard to letter case. Every other source_code_type, including HCPCS and a source-system-specific name, is not applicable to this code-validity test.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["source_code_type", "source_code"]
        },
        {
            "test_name": "patient__sex_null",
            "display_name": "sex is null",
            "description": "Checks whether sex is null in the patient Input Layer Model.",
            "test_type": "missing",
            "severity": 3,
            "affected_columns": ["sex"]
        },
        {
            "test_name": "patient__sex_invalid",
            "display_name": "sex is invalid",
            "description": "Checks whether sex in the patient Input Layer Model is populated with a value other than the exact lowercase values male, female, or unknown.",
            "test_type": "invalid",
            "severity": 3,
            "affected_columns": ["sex"]
        },
        {
            "test_name": "patient__race_invalid",
            "display_name": "race is invalid",
            "description": "Checks whether race in the patient Input Layer Model is populated but not found in Tuva race terminology.",
            "test_type": "invalid",
            "severity": 3,
            "affected_columns": ["race"]
        },
        {
            "test_name": "patient__ethnicity_invalid",
            "display_name": "ethnicity is invalid",
            "description": "Checks whether ethnicity in the patient Input Layer Model is populated but not found in Tuva ethnicity terminology.",
            "test_type": "invalid",
            "severity": 3,
            "affected_columns": ["ethnicity"]
        },
        {
            "test_name": "patient__birth_date_null",
            "display_name": "birth_date is null",
            "description": "Checks whether birth_date is null in the patient Input Layer Model.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["birth_date"]
        },
        {
            "test_name": "patient__birth_date_out_of_range",
            "display_name": "birth_date is out of range",
            "description": "Checks whether birth_date in the patient Input Layer Model is before 1900-01-01 or after the current date.",
            "test_type": "temporal",
            "severity": 2,
            "affected_columns": ["birth_date"]
        },
        {
            "test_name": "patient__death_date_out_of_range",
            "display_name": "death_date is out of range",
            "description": "Checks whether death_date in the patient Input Layer Model is before 1900-01-01 or after the current date.",
            "test_type": "temporal",
            "severity": 2,
            "affected_columns": ["death_date"]
        },
        {
            "test_name": "patient__birth_date_outside_supported_date_range",
            "display_name": "birth_date is outside the supported date range",
            "description": "Checks whether a populated birth_date in the patient Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["birth_date"]
        },
        {
            "test_name": "patient__death_date_outside_supported_date_range",
            "display_name": "death_date is outside the supported date range",
            "description": "Checks whether a populated death_date in the patient Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["death_date"]
        },
        {
            "test_name": "patient__birth_date_after_death_date",
            "display_name": "birth_date is after death_date",
            "description": "Checks whether birth_date is after death_date in the patient Input Layer Model.",
            "test_type": "temporal",
            "severity": 2,
            "affected_columns": ["birth_date", "death_date"]
        },
        {
            "test_name": "patient__death_flag_invalid",
            "display_name": "death_flag is invalid",
            "description": "Checks whether death_flag in the patient Input Layer Model is populated with a value other than 0 or 1.",
            "test_type": "invalid",
            "severity": 3,
            "affected_columns": ["death_flag"]
        },
        {
            "test_name": "patient__death_flag_without_death_date",
            "display_name": "death_flag indicates death without death_date",
            "description": "Checks whether death_flag is 1 in the patient Input Layer Model while death_date is null.",
            "test_type": "consistency",
            "severity": 3,
            "affected_columns": ["death_date", "death_flag"]
        },
        {
            "test_name": "patient__death_date_without_death_flag",
            "display_name": "death_date populated without death_flag",
            "description": "Checks whether death_date is populated in the patient Input Layer Model while death_flag is null or 0.",
            "test_type": "consistency",
            "severity": 3,
            "affected_columns": ["death_date", "death_flag"]
        },
        {
            "test_name": "patient__state_invalid",
            "display_name": "state is invalid",
            "description": "Checks whether state is populated in the patient Input Layer Model but does not match an ANSI/FIPS state abbreviation, state name, or state code.",
            "test_type": "invalid",
            "severity": 3,
            "affected_columns": ["state"]
        },
        {
            "test_name": "patient__zip_code_invalid_format",
            "display_name": "zip_code has an invalid format",
            "description": "Checks whether zip_code is populated in the patient Input Layer Model but is not a 5-digit ZIP code, 9-digit ZIP code, or ZIP+4 value.",
            "test_type": "invalid",
            "severity": 3,
            "affected_columns": ["zip_code"]
        },
        {
            "test_name": "patient__multiple_sexes_per_person",
            "display_name": "sex has multiple values per person_id",
            "description": "Checks whether the same person_id and data_source identify records with more than one exact non-null sex value in the patient Input Layer Model. Letter-case variants are distinct values.",
            "test_type": "consistency",
            "severity": 3,
            "affected_columns": ["sex"]
        },
        {
            "test_name": "patient__multiple_birth_dates_per_person",
            "display_name": "birth_date has multiple values per person_id",
            "description": "Checks whether the same person_id and data_source identify records with more than one non-null birth_date in the patient Input Layer Model.",
            "test_type": "consistency",
            "severity": 2,
            "affected_columns": ["birth_date"]
        },
        {
            "test_name": "practitioner__npi_invalid",
            "display_name": "NPI is invalid",
            "description": "Checks whether npi is populated in the practitioner Input Layer Model but not found in Tuva provider data.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["npi"]
        },
        {
            "test_name": "practitioner__npi_not_individual",
            "display_name": "NPI is not assigned to an individual",
            "description": "Checks whether npi in the practitioner Input Layer Model is found in Tuva provider data but has an NPPES entity type other than individual.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["npi"]
        },
        {
            "test_name": "procedure__person_id_null",
            "display_name": "person_id is null",
            "description": "Checks whether person_id is null in the procedure Input Layer Model.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["person_id"]
        },
        {
            "test_name": "procedure__patient_id_null",
            "display_name": "patient_id is null",
            "description": "Checks whether patient_id is null in the procedure Input Layer Model.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["patient_id"]
        },
        {
            "test_name": "procedure__person_id_not_in_patient",
            "display_name": "person_id not found in patient table",
            "description": "Checks whether person_id values in the procedure Input Layer Model have a corresponding person_id in the patient Input Layer Model for the same data_source.",
            "test_type": "referential",
            "severity": 2,
            "affected_columns": ["person_id", "data_source"]
        },
        {
            "test_name": "procedure__patient_id_not_in_patient",
            "display_name": "patient_id not found in patient table",
            "description": "Checks whether patient_id values in the procedure Input Layer Model have a corresponding patient_id in the patient Input Layer Model for the same data_source.",
            "test_type": "referential",
            "severity": 2,
            "affected_columns": ["patient_id", "data_source"]
        },
        {
            "test_name": "procedure__person_patient_pair_not_in_patient",
            "display_name": "person_id and patient_id pair not found in patient table",
            "description": "Checks whether populated person_id and patient_id values each exist in the patient Input Layer Model for the same data_source but their exact pair does not.",
            "test_type": "referential",
            "severity": 2,
            "affected_columns": ["person_id", "patient_id", "data_source"]
        },
        {
            "test_name": "procedure__encounter_id_not_in_encounter",
            "display_name": "encounter_id not found in encounter table",
            "description": "Checks whether populated encounter_id values in the procedure Input Layer Model have a corresponding encounter_id in the encounter Input Layer Model for the same data_source.",
            "test_type": "referential",
            "severity": 2,
            "affected_columns": ["encounter_id", "data_source"]
        },
        {
            "test_name": "procedure__encounter_person_patient_pair_not_in_encounter",
            "display_name": "encounter_id, person_id, and patient_id combination not found in encounter table",
            "description": "Checks whether a populated encounter_id exists for the data_source and the populated person_id and patient_id pair exists in the patient Input Layer Model, but the encounter is not assigned to that exact pair in the encounter Input Layer Model.",
            "test_type": "referential",
            "severity": 2,
            "affected_columns": ["encounter_id", "person_id", "patient_id", "data_source"]
        },
        {
            "test_name": "procedure__practitioner_id_not_in_practitioner",
            "display_name": "practitioner_id not found in practitioner table",
            "description": "Checks whether populated practitioner_id values in the procedure Input Layer Model have a corresponding practitioner_id in the practitioner Input Layer Model for the same data_source.",
            "test_type": "referential",
            "severity": 3,
            "affected_columns": ["practitioner_id", "data_source"]
        },
        {
            "test_name": "procedure__procedure_date_null",
            "display_name": "procedure_date is null",
            "description": "Checks whether procedure_date is null in the procedure Input Layer Model.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["procedure_date"]
        },
        {
            "test_name": "procedure__procedure_date_out_of_range",
            "display_name": "procedure_date is out of range",
            "description": "Checks whether procedure_date in the procedure Input Layer Model is before 2000-01-01 or after the current date.",
            "test_type": "temporal",
            "severity": 2,
            "affected_columns": ["procedure_date"]
        },
        {
            "test_name": "procedure__procedure_date_outside_supported_date_range",
            "display_name": "procedure_date is outside the supported date range",
            "description": "Checks whether a populated procedure_date in the procedure Input Layer Model is before 1900-01-01 or after 2100-12-31. Structural Data Quality validates the native SQL DATE type; this Logical test does not inspect a serialized string format.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["procedure_date"]
        },
        {
            "test_name": "procedure__code_system_null",
            "display_name": "code_system is null",
            "description": "Checks whether code_system is null in the procedure Input Layer Model.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["code_system"]
        },
        {
            "test_name": "procedure__code_system_invalid",
            "display_name": "code_system is invalid",
            "description": "Checks whether code_system in the procedure Input Layer Model is populated with a value other than icd-10-pcs, icd-9-pcs, hcpcs, or snomed-ct without regard to letter case.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["code_system"]
        },
        {
            "test_name": "procedure__source_code_null",
            "display_name": "source_code is null",
            "description": "Checks whether source_code is null in the procedure Input Layer Model.",
            "test_type": "missing",
            "severity": 2,
            "affected_columns": ["source_code"]
        },
        {
            "test_name": "procedure__source_code_invalid",
            "display_name": "source_code is invalid",
            "description": "Checks whether a populated source_code is absent from the corresponding terminology when code_system identifies ICD-10-PCS, ICD-9-PCS, or SNOMED CT without regard to letter case. Every other code_system, including HCPCS, is not applicable to this code-validity test.",
            "test_type": "invalid",
            "severity": 2,
            "affected_columns": ["code_system", "source_code"]
        }
    ] %}
    {# END LOGICAL TEST DEFINITIONS #}

    {{ return(test_definitions) }}
{% endmacro %}

{% macro dq_validate_logical_test_registry(grouped_definitions, test_definitions) %}
    {% set required_test_fields = [
        'test_name',
        'display_name',
        'description',
        'test_type',
        'severity',
        'affected_columns'
    ] %}
    {% set allowed_test_types = ['missing', 'invalid', 'temporal', 'consistency', 'referential'] %}
    {% set allowed_severities = [1, 2, 3] %}
    {% set definitions_by_name = {} %}
    {% set registry_test_names = [] %}
    {% set enabled_input_model_names = dq_enabled_input_layer_model_names() %}

    {% for definition in test_definitions %}
        {% set missing_fields = [] %}
        {% for field_name in required_test_fields %}
            {% if field_name not in definition %}
                {% do missing_fields.append(field_name) %}
            {% endif %}
        {% endfor %}

        {% if missing_fields | length > 0 %}
            {{ exceptions.raise_compiler_error(
                "Logical Data Quality test metadata is missing required fields: "
                ~ (missing_fields | join(', '))
                ~ "."
            ) }}
        {% endif %}

        {% set test_name = definition['test_name'] | trim %}
        {% set display_name = definition['display_name'] | trim %}
        {% set description = definition['description'] | trim %}

        {% if test_name == '' or test_name.split('__') | length != 2 %}
            {{ exceptions.raise_compiler_error(
                "Logical Data Quality test_name values must use the '<input_table>__<test>' format. Found '"
                ~ test_name
                ~ "'."
            ) }}
        {% endif %}

        {% if test_name in registry_test_names %}
            {{ exceptions.raise_compiler_error(
                "Logical Data Quality test_name '"
                ~ test_name
                ~ "' is declared more than once in dq_logical_test_definitions()."
            ) }}
        {% endif %}
        {% do registry_test_names.append(test_name) %}
        {% do definitions_by_name.update({test_name: definition}) %}

        {% if display_name == '' or display_name == test_name or '__' in display_name %}
            {{ exceptions.raise_compiler_error(
                "Logical Data Quality test '"
                ~ test_name
                ~ "' must declare a human-readable display_name."
            ) }}
        {% endif %}

        {% if description == '' or description[-1] != '.' %}
            {{ exceptions.raise_compiler_error(
                "Logical Data Quality test '"
                ~ test_name
                ~ "' must declare a complete-sentence description."
            ) }}
        {% endif %}

        {% if definition['test_type'] not in allowed_test_types %}
            {{ exceptions.raise_compiler_error(
                "Logical Data Quality test '"
                ~ test_name
                ~ "' declares unsupported test_type '"
                ~ definition['test_type']
                ~ "'. Allowed values are "
                ~ (allowed_test_types | join(', '))
                ~ "."
            ) }}
        {% endif %}

        {% if definition['severity'] not in allowed_severities %}
            {{ exceptions.raise_compiler_error(
                "Logical Data Quality test '"
                ~ test_name
                ~ "' declares unsupported severity '"
                ~ definition['severity']
                ~ "'. Allowed values are 1, 2, and 3."
            ) }}
        {% endif %}

        {% if definition['affected_columns'] is string or definition['affected_columns'] | length == 0 %}
            {{ exceptions.raise_compiler_error(
                "Logical Data Quality test '"
                ~ test_name
                ~ "' must declare at least one affected Input Layer column."
            ) }}
        {% endif %}

        {% set normalized_affected_columns = [] %}
        {% for column_name in definition['affected_columns'] %}
            {% set normalized_column_name = column_name | lower | trim %}
            {% if normalized_column_name == '' %}
                {{ exceptions.raise_compiler_error(
                    "Logical Data Quality test '"
                    ~ test_name
                    ~ "' contains an empty affected column name."
                ) }}
            {% endif %}
            {% if normalized_column_name in normalized_affected_columns %}
                {{ exceptions.raise_compiler_error(
                    "Logical Data Quality test '"
                    ~ test_name
                    ~ "' declares affected column '"
                    ~ normalized_column_name
                    ~ "' more than once."
                ) }}
            {% endif %}
            {% do normalized_affected_columns.append(normalized_column_name) %}
        {% endfor %}
    {% endfor %}

    {% set manifest_test_names = [] %}
    {% set model_flag_pairs = [] %}
    {% set grouped_source_model_names = [] %}
    {% set grouped_flag_table_names = [] %}

    {% for group in grouped_definitions %}
        {% set required_group_fields = [
            'source_model_name',
            'input_model_name',
            'input_table_name',
            'flag_table_name',
            'grain',
            'key_columns',
            'test_names'
        ] %}
        {% for field_name in required_group_fields %}
            {% if field_name not in group %}
                {{ exceptions.raise_compiler_error(
                    "Logical Data Quality group metadata is missing required field '"
                    ~ field_name
                    ~ "'."
                ) }}
            {% endif %}
        {% endfor %}

        {% if group['source_model_name'] | trim == ''
              or group['input_model_name'] | trim == ''
              or group['input_table_name'] | trim == ''
              or group['flag_table_name'] | trim == ''
              or group['grain'] | trim == '' %}
            {{ exceptions.raise_compiler_error(
                "Logical Data Quality group metadata must declare nonempty model, table, and grain values."
            ) }}
        {% endif %}

        {% if group['source_model_name'] in grouped_source_model_names %}
            {{ exceptions.raise_compiler_error(
                "Logical Data Quality source model '"
                ~ group['source_model_name']
                ~ "' is declared by more than one flag-model group."
            ) }}
        {% endif %}
        {% do grouped_source_model_names.append(group['source_model_name']) %}

        {% if group['flag_table_name'] in grouped_flag_table_names %}
            {{ exceptions.raise_compiler_error(
                "Logical Data Quality flag table '"
                ~ group['flag_table_name']
                ~ "' is declared by more than one flag-model group."
            ) }}
        {% endif %}
        {% do grouped_flag_table_names.append(group['flag_table_name']) %}

        {% if group['key_columns'] is string or group['key_columns'] | length == 0 %}
            {{ exceptions.raise_compiler_error(
                "Logical Data Quality flag model '"
                ~ group['source_model_name']
                ~ "' must declare at least one key column."
            ) }}
        {% endif %}

        {% set normalized_key_columns = [] %}
        {% set data_source_key_count = namespace(value=0) %}
        {% for key_column in group['key_columns'] %}
            {% set normalized_key_column = key_column | lower | trim %}
            {% if normalized_key_column == '' or key_column != normalized_key_column %}
                {{ exceptions.raise_compiler_error(
                    "Logical Data Quality flag model '"
                    ~ group['source_model_name']
                    ~ "' must declare nonempty, lowercase key column names. Found '"
                    ~ key_column
                    ~ "'."
                ) }}
            {% endif %}
            {% if normalized_key_column in normalized_key_columns %}
                {{ exceptions.raise_compiler_error(
                    "Logical Data Quality flag model '"
                    ~ group['source_model_name']
                    ~ "' declares key column '"
                    ~ normalized_key_column
                    ~ "' more than once."
                ) }}
            {% endif %}
            {% do normalized_key_columns.append(normalized_key_column) %}
            {% if normalized_key_column == 'data_source' %}
                {% set data_source_key_count.value = data_source_key_count.value + 1 %}
            {% endif %}
        {% endfor %}

        {% if data_source_key_count.value != 1 %}
            {{ exceptions.raise_compiler_error(
                "Logical Data Quality flag model '"
                ~ group['source_model_name']
                ~ "' must include data_source exactly once in key_columns."
            ) }}
        {% endif %}

        {% if group['test_names'] is string or group['test_names'] | length == 0 %}
            {{ exceptions.raise_compiler_error(
                "Logical Data Quality flag model '"
                ~ group['source_model_name']
                ~ "' must declare at least one test_name."
            ) }}
        {% endif %}

        {% for test_name in group['test_names'] %}
            {% if test_name in manifest_test_names %}
                {{ exceptions.raise_compiler_error(
                    "Logical Data Quality test_name '"
                    ~ test_name
                    ~ "' is assigned to more than one flag-model group."
                ) }}
            {% endif %}
            {% do manifest_test_names.append(test_name) %}

            {% if test_name not in definitions_by_name %}
                {{ exceptions.raise_compiler_error(
                    "Logical Data Quality test_name '"
                    ~ test_name
                    ~ "' has group metadata but no explicit test metadata."
                ) }}
            {% endif %}

            {% if not test_name.startswith(group['input_table_name'] ~ '__') %}
                {{ exceptions.raise_compiler_error(
                    "Logical Data Quality test_name '"
                    ~ test_name
                    ~ "' does not match Input Layer table '"
                    ~ group['input_table_name']
                    ~ "'."
                ) }}
            {% endif %}

            {% set model_flag_pair = group['source_model_name'] ~ '|' ~ dq_logical_flag_column_name(test_name) %}
            {% if model_flag_pair in model_flag_pairs %}
                {{ exceptions.raise_compiler_error(
                    "Logical Data Quality flag '"
                    ~ model_flag_pair
                    ~ "' is assigned to more than one test."
                ) }}
            {% endif %}
            {% do model_flag_pairs.append(model_flag_pair) %}
        {% endfor %}

        {% if execute and group['input_model_name'] in enabled_input_model_names %}
            {% set input_model_node = dq_find_model_node(group['input_model_name']) %}
            {% if input_model_node is none %}
                {{ exceptions.raise_compiler_error(
                    "Logical Data Quality could not find parsed Input Layer Wrapper metadata for '"
                    ~ group['input_model_name']
                    ~ "'."
                ) }}
            {% endif %}

            {% set input_column_names = [] %}
            {% for column in input_model_node.columns.values() %}
                {% do input_column_names.append(column.name | lower) %}
            {% endfor %}

            {% for key_column in group['key_columns'] %}
                {% if key_column | lower not in input_column_names %}
                    {{ exceptions.raise_compiler_error(
                        "Logical Data Quality flag model '"
                        ~ group['source_model_name']
                        ~ "' declares key column '"
                        ~ key_column
                        ~ "', which is not declared by Input Layer Wrapper '"
                        ~ group['input_model_name']
                        ~ "'."
                    ) }}
                {% endif %}
            {% endfor %}

            {% for test_name in group['test_names'] %}
                {% for column_name in definitions_by_name[test_name]['affected_columns'] %}
                    {% if column_name | lower not in input_column_names %}
                        {{ exceptions.raise_compiler_error(
                            "Logical Data Quality test '"
                            ~ test_name
                            ~ "' declares affected column '"
                            ~ column_name
                            ~ "', which is not declared by Input Layer Wrapper '"
                            ~ group['input_model_name']
                            ~ "'."
                        ) }}
                    {% endif %}
                {% endfor %}
            {% endfor %}
        {% endif %}
    {% endfor %}

    {% for test_name in registry_test_names %}
        {% if test_name not in manifest_test_names %}
            {{ exceptions.raise_compiler_error(
                "Logical Data Quality test_name '"
                ~ test_name
                ~ "' has explicit test metadata but is not assigned to a flag-model group."
            ) }}
        {% endif %}
    {% endfor %}

    {{ return(definitions_by_name) }}
{% endmacro %}

{% macro dq_logical_test_manifest() %}
    {% set grouped_definitions = [
        {
            'source_model_name': 'data_quality__eligibility_span_flags',
            'input_model_name': 'input_layer__eligibility',
            'input_table_name': 'eligibility',
            'flag_table_name': 'eligibility_span_flags',
            'grain': 'eligibility span',
            'key_columns': ['person_id', 'member_id', 'enrollment_start_date', 'payer', 'plan', 'data_source'],
            'test_names': [
                'eligibility__sex_null',
                'eligibility__sex_invalid',
                'eligibility__race_null',
                'eligibility__race_invalid',
                'eligibility__birth_date_null',
                'eligibility__birth_date_after_death_date',
                'eligibility__birth_date_out_of_reasonable_range',
                'eligibility__death_date_out_of_reasonable_range',
                'eligibility__birth_date_outside_supported_date_range',
                'eligibility__death_date_outside_supported_date_range',
                'eligibility__enrollment_start_date_outside_supported_date_range',
                'eligibility__enrollment_end_date_outside_supported_date_range',
                'eligibility__file_date_outside_supported_date_range',
                'eligibility__death_flag_invalid',
                'eligibility__death_flag_without_death_date',
                'eligibility__enrollment_start_after_end',
                'eligibility__overlapping_enrollment_spans',
                'eligibility__multiple_open_enrollment_spans',
                'eligibility__payer_type_null',
                'eligibility__payer_type_invalid'
            ]
        },
        {
            'source_model_name': 'data_quality__eligibility_person_flags',
            'input_model_name': 'input_layer__eligibility',
            'input_table_name': 'eligibility',
            'flag_table_name': 'eligibility_person_flags',
            'grain': 'person',
            'key_columns': ['person_id', 'data_source'],
            'test_names': [
                'eligibility__multiple_sexes_per_person',
                'eligibility__multiple_races_per_person',
                'eligibility__multiple_birth_dates_per_person'
            ]
        },
        {
            'source_model_name': 'data_quality__medical_claim_line_flags',
            'input_model_name': 'input_layer__medical_claim',
            'input_table_name': 'medical_claim',
            'flag_table_name': 'medical_claim_line_flags',
            'grain': 'medical claim line',
            'key_columns': ['claim_id', 'claim_line_number', 'data_source'],
            'test_names': [
                'medical_claim__claim_line_number_not_positive',
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
                'medical_claim__claim_start_date_outside_supported_date_range',
                'medical_claim__claim_end_date_outside_supported_date_range',
                'medical_claim__claim_line_start_date_outside_supported_date_range',
                'medical_claim__claim_line_end_date_outside_supported_date_range',
                'medical_claim__claim_start_after_claim_end',
                'medical_claim__claim_line_start_after_claim_line_end',
                'medical_claim__admission_date_after_discharge_date',
                'medical_claim__admission_date_out_of_reasonable_range',
                'medical_claim__admission_date_outside_supported_date_range',
                'medical_claim__admission_date_null_for_inpatient_claim',
                'medical_claim__discharge_date_null_for_inpatient_claim',
                'medical_claim__discharge_date_out_of_reasonable_range',
                'medical_claim__discharge_date_outside_supported_date_range',
                'medical_claim__paid_date_null',
                'medical_claim__paid_date_out_of_reasonable_range',
                'medical_claim__paid_date_outside_supported_date_range',
                'medical_claim__paid_date_before_claim_end_date',
                'medical_claim__procedure_date_1_outside_supported_date_range',
                'medical_claim__procedure_date_2_outside_supported_date_range',
                'medical_claim__procedure_date_3_outside_supported_date_range',
                'medical_claim__procedure_date_4_outside_supported_date_range',
                'medical_claim__procedure_date_5_outside_supported_date_range',
                'medical_claim__procedure_date_6_outside_supported_date_range',
                'medical_claim__procedure_date_7_outside_supported_date_range',
                'medical_claim__procedure_date_8_outside_supported_date_range',
                'medical_claim__procedure_date_9_outside_supported_date_range',
                'medical_claim__procedure_date_10_outside_supported_date_range',
                'medical_claim__procedure_date_11_outside_supported_date_range',
                'medical_claim__procedure_date_12_outside_supported_date_range',
                'medical_claim__procedure_date_13_outside_supported_date_range',
                'medical_claim__procedure_date_14_outside_supported_date_range',
                'medical_claim__procedure_date_15_outside_supported_date_range',
                'medical_claim__procedure_date_16_outside_supported_date_range',
                'medical_claim__procedure_date_17_outside_supported_date_range',
                'medical_claim__procedure_date_18_outside_supported_date_range',
                'medical_claim__procedure_date_19_outside_supported_date_range',
                'medical_claim__procedure_date_20_outside_supported_date_range',
                'medical_claim__procedure_date_21_outside_supported_date_range',
                'medical_claim__procedure_date_22_outside_supported_date_range',
                'medical_claim__procedure_date_23_outside_supported_date_range',
                'medical_claim__procedure_date_24_outside_supported_date_range',
                'medical_claim__procedure_date_25_outside_supported_date_range',
                'medical_claim__file_date_outside_supported_date_range',
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
                'medical_claim__procedure_code_1_to_25_invalid',
                'medical_claim__no_matching_eligibility_span'
            ]
        },
        {
            'source_model_name': 'data_quality__medical_claim_claim_flags',
            'input_model_name': 'input_layer__medical_claim',
            'input_table_name': 'medical_claim',
            'flag_table_name': 'medical_claim_claim_flags',
            'grain': 'medical claim',
            'key_columns': ['claim_id', 'data_source'],
            'test_names': [
                'medical_claim__claim_type_count_ne_one_per_claim',
                'medical_claim__multiple_person_ids_per_claim',
                'medical_claim__member_id_has_multiple_values_per_claim',
                'medical_claim__payer_has_multiple_values_per_claim',
                'medical_claim__plan_has_multiple_values_per_claim',
                'medical_claim__claim_start_date_has_multiple_values_per_claim',
                'medical_claim__claim_end_date_has_multiple_values_per_claim',
                'medical_claim__billing_npi_has_multiple_values_per_claim',
                'medical_claim__facility_npi_has_multiple_values_per_claim',
                'medical_claim__admission_date_has_multiple_values_per_inpatient_claim',
                'medical_claim__discharge_date_has_multiple_values_per_inpatient_claim',
                'medical_claim__admit_source_code_has_multiple_values_per_inpatient_claim',
                'medical_claim__admit_type_code_has_multiple_values_per_inpatient_claim',
                'medical_claim__discharge_disposition_code_has_multiple_values_per_inpatient_claim',
                'medical_claim__bill_type_code_count_ne_one_for_institutional_claim',
                'medical_claim__drg_code_count_ne_one_for_acute_inpatient_claim',
                'medical_claim__drg_code_type_has_multiple_values_per_acute_inpatient_claim',
                'medical_claim__diagnosis_code_type_has_multiple_values_per_claim',
                'medical_claim__procedure_code_type_has_multiple_values_per_claim',
                'medical_claim__diagnosis_code_count_gt_one_per_position_for_institutional_claim'
            ]
        },
        {
            'source_model_name': 'data_quality__pharmacy_claim_line_flags',
            'input_model_name': 'input_layer__pharmacy_claim',
            'input_table_name': 'pharmacy_claim',
            'flag_table_name': 'pharmacy_claim_line_flags',
            'grain': 'pharmacy claim line',
            'key_columns': ['claim_id', 'claim_line_number', 'data_source'],
            'test_names': [
                'pharmacy_claim__claim_line_number_not_positive',
                'pharmacy_claim__person_id_null',
                'pharmacy_claim__dispensing_date_null',
                'pharmacy_claim__paid_date_null',
                'pharmacy_claim__dispensing_date_out_of_reasonable_range',
                'pharmacy_claim__paid_date_out_of_reasonable_range',
                'pharmacy_claim__dispensing_date_outside_supported_date_range',
                'pharmacy_claim__paid_date_outside_supported_date_range',
                'pharmacy_claim__file_date_outside_supported_date_range',
                'pharmacy_claim__prescribing_provider_npi_null',
                'pharmacy_claim__prescribing_provider_npi_invalid',
                'pharmacy_claim__dispensing_provider_npi_null',
                'pharmacy_claim__dispensing_provider_npi_invalid',
                'pharmacy_claim__ndc_code_null',
                'pharmacy_claim__ndc_code_invalid',
                'pharmacy_claim__quantity_null',
                'pharmacy_claim__quantity_not_positive',
                'pharmacy_claim__days_supply_null',
                'pharmacy_claim__days_supply_not_positive',
                'pharmacy_claim__refills_lt_zero',
                'pharmacy_claim__paid_amount_null',
                'pharmacy_claim__paid_amount_lt_zero',
                'pharmacy_claim__allowed_amount_null',
                'pharmacy_claim__allowed_amount_lt_zero',
                'pharmacy_claim__paid_amount_gt_allowed_amount',
                'pharmacy_claim__no_matching_eligibility_span'
            ]
        },
        {
            'source_model_name': 'data_quality__pharmacy_claim_claim_flags',
            'input_model_name': 'input_layer__pharmacy_claim',
            'input_table_name': 'pharmacy_claim',
            'flag_table_name': 'pharmacy_claim_claim_flags',
            'grain': 'pharmacy claim',
            'key_columns': ['claim_id', 'data_source'],
            'test_names': [
                'pharmacy_claim__multiple_person_ids_per_claim'
            ]
        },
        {
            'source_model_name': 'data_quality__provider_attribution_flags',
            'input_model_name': 'input_layer__provider_attribution',
            'input_table_name': 'provider_attribution',
            'flag_table_name': 'provider_attribution_flags',
            'grain': 'provider attribution record',
            'key_columns': ['person_id', 'member_id', 'year_month', 'payer', 'plan', 'data_source'],
            'test_names': [
                'provider_attribution__year_month_invalid_format'
            ]
        },
        {
            'source_model_name': 'data_quality__appointment_flags',
            'input_model_name': 'input_layer__appointment',
            'input_table_name': 'appointment',
            'flag_table_name': 'appointment_flags',
            'grain': 'appointment record',
            'key_columns': ['appointment_id', 'data_source'],
            'test_names': [
                'appointment__person_id_not_in_patient',
                'appointment__patient_id_not_in_patient',
                'appointment__person_patient_pair_not_in_patient',
                'appointment__encounter_id_not_in_encounter',
                'appointment__encounter_person_patient_pair_not_in_encounter',
                'appointment__start_datetime_null',
                'appointment__end_datetime_before_start_datetime',
                'appointment__duration_negative',
                'appointment__type_code_invalid',
                'appointment__status_code_invalid'
            ]
        },
        {
            'source_model_name': 'data_quality__condition_flags',
            'input_model_name': 'input_layer__condition',
            'input_table_name': 'condition',
            'flag_table_name': 'condition_flags',
            'grain': 'condition record',
            'key_columns': ['source_condition_id', 'data_source'],
            'test_names': [
                'condition__person_id_null',
                'condition__patient_id_null',
                'condition__source_code_null',
                'condition__code_system_null',
                'condition__person_id_not_in_patient',
                'condition__patient_id_not_in_patient',
                'condition__person_patient_pair_not_in_patient',
                'condition__encounter_id_not_in_encounter',
                'condition__encounter_person_patient_pair_not_in_encounter',
                'condition__onset_date_after_resolved_date',
                'condition__recorded_date_out_of_reasonable_range',
                'condition__onset_date_out_of_reasonable_range',
                'condition__resolved_date_out_of_reasonable_range',
                'condition__recorded_date_outside_supported_date_range',
                'condition__onset_date_outside_supported_date_range',
                'condition__resolved_date_outside_supported_date_range',
                'condition__code_system_invalid',
                'condition__source_code_invalid',
                'condition__present_on_admit_code_invalid'
            ]
        },
        {
            'source_model_name': 'data_quality__encounter_flags',
            'input_model_name': 'input_layer__encounter',
            'input_table_name': 'encounter',
            'flag_table_name': 'encounter_flags',
            'grain': 'encounter record',
            'key_columns': ['encounter_id', 'data_source'],
            'test_names': [
                'encounter__person_id_null',
                'encounter__patient_id_null',
                'encounter__person_id_not_in_patient',
                'encounter__patient_id_not_in_patient',
                'encounter__person_patient_pair_not_in_patient',
                'encounter__encounter_type_invalid',
                'encounter__encounter_start_date_null',
                'encounter__encounter_end_date_null',
                'encounter__encounter_start_date_after_encounter_end_date',
                'encounter__encounter_start_date_out_of_reasonable_range',
                'encounter__encounter_end_date_out_of_reasonable_range',
                'encounter__encounter_start_date_outside_supported_date_range',
                'encounter__encounter_end_date_outside_supported_date_range',
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
            'source_model_name': 'data_quality__immunization_flags',
            'input_model_name': 'input_layer__immunization',
            'input_table_name': 'immunization',
            'flag_table_name': 'immunization_flags',
            'grain': 'immunization record',
            'key_columns': ['immunization_id', 'data_source'],
            'test_names': [
                'immunization__person_id_null',
                'immunization__patient_id_null',
                'immunization__person_id_not_in_patient',
                'immunization__patient_id_not_in_patient',
                'immunization__person_patient_pair_not_in_patient',
                'immunization__encounter_id_not_in_encounter',
                'immunization__encounter_person_patient_pair_not_in_encounter',
                'immunization__occurrence_date_null',
                'immunization__occurrence_date_outside_supported_date_range',
                'immunization__source_code_type_null_when_source_code_present',
                'immunization__source_code_null_when_source_code_type_present',
                'immunization__source_code_invalid',
                'immunization__status_invalid',
                'immunization__status_reason_invalid',
                'immunization__status_reason_present_for_completed_or_error_status'
            ]
        },
        {
            'source_model_name': 'data_quality__lab_result_flags',
            'input_model_name': 'input_layer__lab_result',
            'input_table_name': 'lab_result',
            'flag_table_name': 'lab_result_flags',
            'grain': 'lab result record',
            'key_columns': ['lab_result_id', 'data_source'],
            'test_names': [
                'lab_result__person_id_null',
                'lab_result__patient_id_null',
                'lab_result__person_id_not_in_patient',
                'lab_result__patient_id_not_in_patient',
                'lab_result__person_patient_pair_not_in_patient',
                'lab_result__encounter_id_not_in_encounter',
                'lab_result__encounter_person_patient_pair_not_in_encounter',
                'lab_result__accession_number_null',
                'lab_result__source_component_type_null_when_source_component_code_present',
                'lab_result__source_component_code_invalid',
                'lab_result__source_order_type_null_when_source_order_code_present',
                'lab_result__source_order_code_null_when_source_order_type_present',
                'lab_result__source_order_code_invalid',
                'lab_result__collection_datetime_after_result_datetime',
                'lab_result__result_datetime_out_of_reasonable_range',
                'lab_result__collection_datetime_out_of_reasonable_range'
            ]
        },
        {
            'source_model_name': 'data_quality__location_flags',
            'input_model_name': 'input_layer__location',
            'input_table_name': 'location',
            'flag_table_name': 'location_flags',
            'grain': 'location record',
            'key_columns': ['location_id', 'data_source'],
            'test_names': [
                'location__npi_invalid'
            ]
        },
        {
            'source_model_name': 'data_quality__medication_flags',
            'input_model_name': 'input_layer__medication',
            'input_table_name': 'medication',
            'flag_table_name': 'medication_flags',
            'grain': 'medication record',
            'key_columns': ['medication_id', 'data_source'],
            'test_names': [
                'medication__person_id_null',
                'medication__patient_id_null',
                'medication__person_id_not_in_patient',
                'medication__patient_id_not_in_patient',
                'medication__person_patient_pair_not_in_patient',
                'medication__encounter_id_not_in_encounter',
                'medication__encounter_person_patient_pair_not_in_encounter',
                'medication__practitioner_id_not_in_practitioner',
                'medication__dispensing_date_out_of_range',
                'medication__prescribing_date_out_of_range',
                'medication__dispensing_date_outside_supported_date_range',
                'medication__prescribing_date_outside_supported_date_range',
                'medication__prescribing_date_after_dispensing_date',
                'medication__source_code_type_null_when_source_code_present',
                'medication__source_code_null',
                'medication__source_code_invalid',
                'medication__ndc_code_invalid',
                'medication__quantity_negative',
                'medication__days_supply_negative'
            ]
        },
        {
            'source_model_name': 'data_quality__observation_flags',
            'input_model_name': 'input_layer__observation',
            'input_table_name': 'observation',
            'flag_table_name': 'observation_flags',
            'grain': 'observation record',
            'key_columns': ['observation_id', 'data_source'],
            'test_names': [
                'observation__person_id_null',
                'observation__patient_id_null',
                'observation__person_id_not_in_patient',
                'observation__patient_id_not_in_patient',
                'observation__person_patient_pair_not_in_patient',
                'observation__encounter_id_not_in_encounter',
                'observation__encounter_person_patient_pair_not_in_encounter',
                'observation__observation_date_null',
                'observation__observation_date_out_of_range',
                'observation__observation_date_outside_supported_date_range',
                'observation__observation_type_invalid',
                'observation__source_code_type_null_when_source_code_present',
                'observation__source_code_null',
                'observation__source_code_invalid'
            ]
        },
        {
            'source_model_name': 'data_quality__patient_flags',
            'input_model_name': 'input_layer__patient',
            'input_table_name': 'patient',
            'flag_table_name': 'patient_flags',
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
                'patient__birth_date_outside_supported_date_range',
                'patient__death_date_outside_supported_date_range',
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
            'source_model_name': 'data_quality__practitioner_flags',
            'input_model_name': 'input_layer__practitioner',
            'input_table_name': 'practitioner',
            'flag_table_name': 'practitioner_flags',
            'grain': 'practitioner record',
            'key_columns': ['practitioner_id', 'data_source'],
            'test_names': [
                'practitioner__npi_invalid',
                'practitioner__npi_not_individual'
            ]
        },
        {
            'source_model_name': 'data_quality__procedure_flags',
            'input_model_name': 'input_layer__procedure',
            'input_table_name': 'procedure',
            'flag_table_name': 'procedure_flags',
            'grain': 'procedure record',
            'key_columns': ['source_procedure_id', 'data_source'],
            'test_names': [
                'procedure__person_id_null',
                'procedure__patient_id_null',
                'procedure__person_id_not_in_patient',
                'procedure__patient_id_not_in_patient',
                'procedure__person_patient_pair_not_in_patient',
                'procedure__encounter_id_not_in_encounter',
                'procedure__encounter_person_patient_pair_not_in_encounter',
                'procedure__practitioner_id_not_in_practitioner',
                'procedure__procedure_date_null',
                'procedure__procedure_date_out_of_range',
                'procedure__procedure_date_outside_supported_date_range',
                'procedure__code_system_null',
                'procedure__code_system_invalid',
                'procedure__source_code_null',
                'procedure__source_code_invalid'
            ]
        }
    ] %}

    {% set test_definitions = dq_logical_test_definitions() %}
    {% set definitions_by_name = dq_validate_logical_test_registry(grouped_definitions, test_definitions) %}
    {% set manifest = [] %}

    {% for definition in grouped_definitions %}
        {% for test_name in definition['test_names'] %}
            {% set test_definition = definitions_by_name[test_name] %}
            {% set registry_entry = {
                'source_model_name': definition['source_model_name'],
                'input_model_name': definition['input_model_name'],
                'input_table_name': definition['input_table_name'],
                'flag_table_name': definition['flag_table_name'],
                'grain': definition['grain'],
                'key_columns': definition['key_columns'],
                'test_name': test_name,
                'flag_column_name': dq_logical_flag_column_name(test_name),
                'display_name': test_definition['display_name'],
                'description': test_definition['description'],
                'test_type': test_definition['test_type'],
                'severity': test_definition['severity'],
                'affected_columns': test_definition['affected_columns']
            } %}
            {% do manifest.append(registry_entry) %}
        {% endfor %}
    {% endfor %}

    {{ return(manifest) }}
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
