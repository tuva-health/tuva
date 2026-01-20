{{
    config(
        enabled = true,
        materialized = 'ephemeral'
    )
}}

{#
    Passthrough Columns Validation Model
    ====================================
    Validates passthrough columns exist in core models.

    Run with: dbt show --select validate_extension_columns --limit 100 --vars '{"passthrough": {"prefix": "x_", "strip": false}}'

    Returns a table showing:
    - model_name: The core model being checked
    - expected_column: The column we expect to find
    - status: PASS if column has data, CHECK if column exists but null
#}

{% set passthrough_config = var('passthrough', {}) %}
{% set passthrough_prefix = passthrough_config.get('prefix', 'x_') %}
{% set passthrough_strip = passthrough_config.get('strip', false) %}

{#
   Expected columns per model - VERIFIED from integration_tests/models/*.sql
   These match the tuva_extensions blocks in each input model
#}
{#
   NOTE: core__patient uses eligibility extensions when both claims+clinical enabled.
   So patient columns match eligibility columns, not patient.sql input model.
#}
{% set x_columns = {
    'eligibility': ['x_temp_person_id', 'x_temp_first_name'],
    'medical_claim': ['x_temp_claim_id', 'x_temp_payer'],
    'pharmacy_claim': ['x_temp_ndc_code'],
    'patient': ['x_temp_person_id', 'x_temp_first_name'],
    'medication': ['x_temp_rxnorm_code', 'x_temp_source_code_type'],
    'condition': ['x_temp_status', 'x_temp_condition_type', 'x_temp_source_code'],
    'procedure': ['x_temp_procedure_id', 'x_temp_person_id'],
    'encounter': ['x_temp_encounter_type', 'x_temp_encounter_start_date'],
    'appointment': ['x_temp_normalized_status', 'x_temp_appointment_specialty'],
    'immunization': ['x_temp_person_id', 'x_temp_source_code'],
    'lab_result': ['x_temp_lab_result_id', 'x_temp_person_id', 'x_temp_source_component_type'],
    'observation': ['x_temp_observation_id'],
    'location': ['x_temp_state', 'x_temp_parent_organization'],
    'practitioner': ['x_temp_specialty', 'x_temp_first_name', 'x_temp_last_name']
} %}

{% set zzz_columns = {
    'eligibility': ['zzz_temp_payer_type'],
    'medical_claim': ['zzz_temp_person_id', 'zzz_temp_paid_date'],
    'pharmacy_claim': ['zzz_temp_prescribing_provider_npi', 'zzz_temp_plan'],
    'patient': ['zzz_temp_payer_type'],
    'medication': ['zzz_temp_source_code'],
    'condition': ['zzz_temp_recorded_date'],
    'procedure': ['zzz_temp_patient_id'],
    'encounter': ['zzz_temp_facility_name'],
    'appointment': ['zzz_temp_start_datetime'],
    'immunization': ['zzz_temp_source_code_type'],
    'lab_result': ['zzz_temp_source_order_type'],
    'observation': ['zzz_temp_observation_date'],
    'location': ['zzz_temp_facility_type'],
    'practitioner': ['zzz_temp_practice_affiliation']
} %}

{# Select which set of columns to check based on prefix #}
{% set columns_to_check = x_columns if passthrough_prefix == 'x_' else zzz_columns %}

{# Build validation queries #}
{% set validation_queries = [] %}

{% for model_name, expected_cols in columns_to_check.items() %}
    {% for col in expected_cols %}
        {# Calculate expected column name (with or without prefix based on strip setting) #}
        {% set search_col = col[passthrough_prefix|length:] if passthrough_strip else col %}
        {% set query %}
select
    '{{ model_name }}' as model_name,
    '{{ col }}' as original_column,
    '{{ search_col }}' as expected_column,
    case when exists (
        select 1 from {{ ref('core__' ~ model_name) }}
        where {{ search_col }} is not null
        limit 1
    ) then 'PASS' else 'CHECK' end as status
        {% endset %}
        {% do validation_queries.append(query) %}
    {% endfor %}
{% endfor %}

{% if validation_queries | length > 0 %}
select * from (
{{ validation_queries | join('\nunion all\n') }}
) results
order by model_name, expected_column
{% else %}
select
    'no_models' as model_name,
    'none' as original_column,
    'none' as expected_column,
    'N/A' as status
{% endif %}
