-- Verifies that the selected extension reaches every supported Core output
-- without being populated from the wrong Input Layer table. Hybrid clinical /
-- claims outputs may contain nulls on claims-derived rows.

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
    {'name': 'appointment', 'relation': ref('core__appointment'), 'input': ref('input_layer__appointment')},
    {'name': 'condition', 'relation': ref('core__condition'), 'input': ref('input_layer__condition')},
    {'name': 'eligibility', 'relation': ref('core__eligibility'), 'input': ref('input_layer__eligibility')},
    {'name': 'encounter', 'relation': ref('core__encounter'), 'input': ref('input_layer__encounter')},
    {'name': 'immunization', 'relation': ref('core__immunization'), 'input': ref('input_layer__immunization')},
    {'name': 'lab_result', 'relation': ref('core__lab_result'), 'input': ref('input_layer__lab_result')},
    {'name': 'location', 'relation': ref('core__location'), 'input': ref('input_layer__location')},
    {'name': 'medical_claim', 'relation': ref('core__medical_claim'), 'input': ref('input_layer__medical_claim')},
    {'name': 'medication', 'relation': ref('core__medication'), 'input': ref('input_layer__medication')},
    {'name': 'observation', 'relation': ref('core__observation'), 'input': ref('input_layer__observation')},
    {'name': 'patient', 'relation': ref('core__patient'), 'input': ref('input_layer__patient')},
    {'name': 'pharmacy_claim', 'relation': ref('core__pharmacy_claim'), 'input': ref('input_layer__pharmacy_claim')},
    {'name': 'practitioner', 'relation': ref('core__practitioner'), 'input': ref('input_layer__practitioner')},
    {'name': 'procedure', 'relation': ref('core__procedure'), 'input': ref('input_layer__procedure')}
] -%}

{%- for item in supported_relations %}
select
      '{{ item['name'] }}' as core_table
    , 'unexpected extension value' as failure_reason
from {{ item['relation'] }}
where {{ expected_column }} is not null
  and cast({{ expected_column }} as {{ dbt.type_string() }}) <> '{{ item['name'] }}'

union all

select
      '{{ item['name'] }}' as core_table
    , 'no populated extension value reached Core' as failure_reason
where exists (select 1 from {{ item['input'] }})
  and not exists (
      select 1
      from {{ item['relation'] }}
      where cast({{ expected_column }} as {{ dbt.type_string() }}) = '{{ item['name'] }}'
  )

    {%- if not loop.last %}
union all
    {%- endif %}
{%- endfor %}
