{% macro metric_testing_claims_or_clinical_enabled() %}
    {% if var('claims_enabled', none) is none %}
        {{ return(var('clinical_enabled', False) | as_bool) }}
    {% endif %}

    {{ return(var('claims_enabled', False) | as_bool) }}
{% endmacro %}


{% macro get_metric_testing_models(model_group) %}
    {% set claims_enabled = var('claims_enabled', False) | as_bool %}
    {% set clinical_enabled = var('clinical_enabled', False) | as_bool %}
    {% set provider_attribution_enabled = var('provider_attribution_enabled', False) | as_bool %}
    {% set claims_or_clinical_enabled = metric_testing_claims_or_clinical_enabled() %}
    {% set ns = namespace(models=[]) %}

    {% if model_group == 'input_layer' %}
        {% if claims_enabled %}
            {% for model_name in [
                'input_layer__eligibility',
                'input_layer__medical_claim',
                'input_layer__pharmacy_claim'
            ] %}
                {% do ns.models.append(model_name) %}
            {% endfor %}
        {% endif %}

        {% if claims_enabled and provider_attribution_enabled %}
            {% do ns.models.append('input_layer__provider_attribution') %}
        {% endif %}

        {% if clinical_enabled %}
            {% for model_name in [
                'input_layer__appointment',
                'input_layer__condition',
                'input_layer__encounter',
                'input_layer__immunization',
                'input_layer__lab_result',
                'input_layer__location',
                'input_layer__medication',
                'input_layer__observation',
                'input_layer__patient',
                'input_layer__practitioner',
                'input_layer__procedure'
            ] %}
                {% do ns.models.append(model_name) %}
            {% endfor %}
        {% endif %}
    {% elif model_group == 'core_final' %}
        {% if claims_enabled %}
            {% for model_name in [
                'core__eligibility',
                'core__medical_claim',
                'core__member_months',
                'core__pharmacy_claim'
            ] %}
                {% do ns.models.append(model_name) %}
            {% endfor %}
        {% endif %}

        {% if claims_enabled or clinical_enabled %}
            {% for model_name in [
                'core__condition',
                'core__encounter',
                'core__location',
                'core__patient',
                'core__practitioner',
                'core__procedure',
                'core__person_id_crosswalk'
            ] %}
                {% do ns.models.append(model_name) %}
            {% endfor %}
        {% endif %}

        {% if clinical_enabled %}
            {% for model_name in [
                'core__appointment',
                'core__immunization',
                'core__lab_result',
                'core__medication',
                'core__observation'
            ] %}
                {% do ns.models.append(model_name) %}
            {% endfor %}
        {% endif %}
    {% elif model_group == 'data_mart_summary' %}
        {% if claims_enabled %}
            {% for model_name in [
                'ahrq_measures__pqi_summary',
                'ed_classification__summary',
                'readmissions__readmission_summary'
            ] %}
                {% do ns.models.append(model_name) %}
            {% endfor %}
        {% endif %}

        {% if claims_or_clinical_enabled %}
            {% for model_name in [
                'ccsr__procedure_summary',
                'hcc_suspecting__summary',
                'quality_measures__summary_counts',
                'quality_measures__summary_long',
                'quality_measures__summary_wide'
            ] %}
                {% do ns.models.append(model_name) %}
            {% endfor %}
        {% endif %}
    {% endif %}

    {{ return(ns.models | sort) }}
{% endmacro %}


{% macro get_metric_testing_input_core_pairs() %}
    {% set input_models = get_metric_testing_models('input_layer') %}
    {% set core_models = get_metric_testing_models('core_final') %}
    {% set ns = namespace(pairs=[]) %}

    {% for input_model in input_models %}
        {% set suffix = input_model | replace('input_layer__', '') %}
        {% set core_model = 'core__' ~ suffix %}

        {% if core_model in core_models %}
            {% do ns.pairs.append({
                'suffix': suffix,
                'input_model': input_model,
                'core_model': core_model
            }) %}
        {% endif %}
    {% endfor %}

    {{ return(ns.pairs) }}
{% endmacro %}


{% macro metric_testing_empty_result() %}
select
    cast(null as {{ dbt.type_string() }}) as metric_id
  , cast(null as {{ dbt.type_string() }}) as metric_name
  , cast(null as {{ dbt.type_string() }}) as metric_description
  , cast(null as {{ dbt.type_numeric() }}) as metric_value
where 1 = 0
{% endmacro %}


{% macro metric_testing_row_count_select(metric_group, model_name) %}
    {% set metric_id = metric_group ~ '__' ~ model_name ~ '__row_count' %}
    {% set metric_name = 'Row count for ' ~ model_name %}
    {% set metric_description = 'Count of rows in ' ~ model_name %}

select
    cast('{{ metric_id }}' as {{ dbt.type_string() }}) as metric_id
  , cast('{{ metric_name }}' as {{ dbt.type_string() }}) as metric_name
  , cast('{{ metric_description }}' as {{ dbt.type_string() }}) as metric_description
  , cast(count(*) as {{ dbt.type_numeric() }}) as metric_value
from {{ ref(model_name) }}
{% endmacro %}


{% macro metric_testing_count_distinct_select(metric_group, model_name, column_name) %}
    {% set metric_id = metric_group ~ '__' ~ model_name ~ '__distinct_' ~ column_name ~ '_count' %}
    {% set metric_name = 'Distinct ' ~ column_name ~ ' count for ' ~ model_name %}
    {% set metric_description = 'Count of distinct ' ~ column_name ~ ' values in ' ~ model_name %}

select
    cast('{{ metric_id }}' as {{ dbt.type_string() }}) as metric_id
  , cast('{{ metric_name }}' as {{ dbt.type_string() }}) as metric_name
  , cast('{{ metric_description }}' as {{ dbt.type_string() }}) as metric_description
  , cast(count(distinct {{ column_name }}) as {{ dbt.type_numeric() }}) as metric_value
from {{ ref(model_name) }}
{% endmacro %}


{% macro metric_testing_input_core_diff_select(suffix, input_model, core_model) %}
    {% set metric_id = 'input_to_core_diff__' ~ suffix ~ '__row_count' %}
    {% set metric_name = 'Row count diff for ' ~ suffix %}
    {% set metric_description = 'Difference between ' ~ core_model ~ ' and ' ~ input_model ~ ' row counts; expected 0' %}

select
    cast('{{ metric_id }}' as {{ dbt.type_string() }}) as metric_id
  , cast('{{ metric_name }}' as {{ dbt.type_string() }}) as metric_name
  , cast('{{ metric_description }}' as {{ dbt.type_string() }}) as metric_description
  , cast(core_metrics.core_row_count - input_metrics.input_row_count as {{ dbt.type_numeric() }}) as metric_value
from (
    select cast(count(*) as {{ dbt.type_numeric() }}) as input_row_count
    from {{ ref(input_model) }}
) as input_metrics
cross join (
    select cast(count(*) as {{ dbt.type_numeric() }}) as core_row_count
    from {{ ref(core_model) }}
) as core_metrics
{% endmacro %}
