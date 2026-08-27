-- Verifies the public extension-column schema contract. Every supported Core
-- output must expose the integration fixture's selected extension column, and
-- derived Core outputs must not expose any integration fixture extension.

{{ config(
     enabled = (the_tuva_project.tuva_boolean_var('claims_enabled', false))
            and (the_tuva_project.tuva_boolean_var('clinical_enabled', false)),
     tags = ['extension_columns'],
     severity = 'error'
   )
}}

{%- set passthrough_config = var('passthrough', {}) -%}
{%- set extension_prefix = passthrough_config.get('prefix', 'x_') -%}
{%- set strip_prefix = passthrough_config.get('strip', false) -%}
{%- set expected_column = 'tuva_test_extension' if strip_prefix else extension_prefix ~ 'tuva_test_extension' -%}

{%- set supported_relations = [
    {'name': 'appointment', 'relation': ref('core__appointment')},
    {'name': 'condition', 'relation': ref('core__condition')},
    {'name': 'eligibility', 'relation': ref('core__eligibility')},
    {'name': 'encounter', 'relation': ref('core__encounter')},
    {'name': 'immunization', 'relation': ref('core__immunization')},
    {'name': 'lab_result', 'relation': ref('core__lab_result')},
    {'name': 'location', 'relation': ref('core__location')},
    {'name': 'medical_claim', 'relation': ref('core__medical_claim')},
    {'name': 'medication', 'relation': ref('core__medication')},
    {'name': 'observation', 'relation': ref('core__observation')},
    {'name': 'patient', 'relation': ref('core__patient')},
    {'name': 'pharmacy_claim', 'relation': ref('core__pharmacy_claim')},
    {'name': 'practitioner', 'relation': ref('core__practitioner')},
    {'name': 'procedure', 'relation': ref('core__procedure')}
] -%}

{%- set derived_relations = [
    {'name': 'cost', 'relation': ref('core__cost')},
    {'name': 'member_month', 'relation': ref('core__member_month')},
    {'name': 'person_id_crosswalk', 'relation': ref('core__person_id_crosswalk')},
    {'name': 'utilization', 'relation': ref('core__utilization')}
] -%}

{%- set fixture_extension_columns = [
    'x_tuva_test_extension',
    'ext_tuva_test_extension',
    'tuva_test_extension',
    'x_temp_record_origin',
    'temp_record_origin',
    'x_temp_person_id',
    'temp_person_id',
    'x_temp_first_name',
    'temp_first_name',
    'x_first_name',
    'ext_first_name',
    'first_name',
    'x_temp_claim_id',
    'temp_claim_id',
    'x_temp_payer',
    'temp_payer',
    'x_temp_ndc_code',
    'temp_ndc_code'
] -%}

{%- set failures = [] -%}
{%- if execute -%}
    {%- for item in supported_relations -%}
        {%- set column_names = [] -%}
        {%- for column in adapter.get_columns_in_relation(item['relation']) -%}
            {%- do column_names.append(column.name.lower()) -%}
        {%- endfor -%}
        {%- if expected_column.lower() not in column_names -%}
            {%- do failures.append(item['name'] ~ ' is missing ' ~ expected_column) -%}
        {%- endif -%}
    {%- endfor -%}

    {%- for item in derived_relations -%}
        {%- set column_names = [] -%}
        {%- for column in adapter.get_columns_in_relation(item['relation']) -%}
            {%- do column_names.append(column.name.lower()) -%}
        {%- endfor -%}
        {%- for extension_column in fixture_extension_columns -%}
            {%- if extension_column in column_names -%}
                {%- do failures.append(item['name'] ~ ' unexpectedly exposes ' ~ extension_column) -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endfor -%}
{%- endif -%}

{%- if failures | length > 0 -%}
    {%- for failure in failures %}
select '{{ failure }}' as failure_reason
        {%- if not loop.last %}
union all
        {%- endif %}
    {%- endfor %}
{%- else %}
select cast(null as {{ dbt.type_string() }}) as failure_reason
where false
{%- endif %}
