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


{% macro get_metric_testing_metric_id_map() %}
    {% set metric_id_map = {
        'input_layer__input_layer__appointment__row_count': '00001',
        'input_layer__input_layer__condition__row_count': '00002',
        'input_layer__input_layer__eligibility__row_count': '00003',
        'input_layer__input_layer__encounter__row_count': '00004',
        'input_layer__input_layer__immunization__row_count': '00005',
        'input_layer__input_layer__lab_result__row_count': '00006',
        'input_layer__input_layer__location__row_count': '00007',
        'input_layer__input_layer__medical_claim__row_count': '00008',
        'input_layer__input_layer__medication__row_count': '00009',
        'input_layer__input_layer__observation__row_count': '00010',
        'input_layer__input_layer__patient__row_count': '00011',
        'input_layer__input_layer__pharmacy_claim__row_count': '00012',
        'input_layer__input_layer__practitioner__row_count': '00013',
        'input_layer__input_layer__procedure__row_count': '00014',
        'input_layer__input_layer__provider_attribution__row_count': '00015',
        'core__core__appointment__row_count': '00016',
        'core__core__condition__row_count': '00017',
        'core__core__eligibility__row_count': '00018',
        'core__core__encounter__row_count': '00019',
        'core__core__immunization__row_count': '00020',
        'core__core__lab_result__row_count': '00021',
        'core__core__location__row_count': '00022',
        'core__core__medical_claim__row_count': '00023',
        'core__core__medication__row_count': '00024',
        'core__core__member_months__row_count': '00025',
        'core__core__observation__row_count': '00026',
        'core__core__patient__row_count': '00027',
        'core__core__person_id_crosswalk__row_count': '00028',
        'core__core__pharmacy_claim__row_count': '00029',
        'core__core__practitioner__row_count': '00030',
        'core__core__procedure__row_count': '00031',
        'input_to_core_diff__appointment__row_count': '00032',
        'input_to_core_diff__condition__row_count': '00033',
        'input_to_core_diff__eligibility__row_count': '00034',
        'input_to_core_diff__encounter__row_count': '00035',
        'input_to_core_diff__immunization__row_count': '00036',
        'input_to_core_diff__lab_result__row_count': '00037',
        'input_to_core_diff__location__row_count': '00038',
        'input_to_core_diff__medical_claim__row_count': '00039',
        'input_to_core_diff__medication__row_count': '00040',
        'input_to_core_diff__observation__row_count': '00041',
        'input_to_core_diff__patient__row_count': '00042',
        'input_to_core_diff__pharmacy_claim__row_count': '00043',
        'input_to_core_diff__practitioner__row_count': '00044',
        'input_to_core_diff__procedure__row_count': '00045',
        'data_mart__ahrq_measures__pqi_summary__row_count': '00046',
        'data_mart__ccsr__procedure_summary__row_count': '00047',
        'data_mart__ed_classification__summary__row_count': '00048',
        'data_mart__hcc_suspecting__summary__row_count': '00049',
        'data_mart__quality_measures__summary_counts__row_count': '00050',
        'data_mart__quality_measures__summary_long__row_count': '00051',
        'data_mart__quality_measures__summary_wide__row_count': '00052',
        'data_mart__readmissions__readmission_summary__row_count': '00053',
        'core__core__medical_claim__distinct_claim_id_count': '00054'
    } %}

    {{ return(metric_id_map) }}
{% endmacro %}


{% macro get_metric_testing_metric_id(metric_key) %}
    {% set metric_id_map = get_metric_testing_metric_id_map() %}

    {% if metric_key not in metric_id_map %}
        {% do exceptions.raise_compiler_error('Missing metric testing ID for metric key: ' ~ metric_key) %}
    {% endif %}

    {{ return(metric_id_map[metric_key]) }}
{% endmacro %}


{% macro metric_testing_empty_result() %}
select
    cast(null as {{ dbt.type_string() }}) as metric_id
  , cast(null as {{ dbt.type_string() }}) as metric_name
  , cast(null as {{ dbt.type_numeric() }}) as metric_value
where 1 = 0
{% endmacro %}


{% macro metric_testing_row_count_select(metric_group, model_name) %}
    {% set metric_key = metric_group ~ '__' ~ model_name ~ '__row_count' %}
    {% set metric_id = get_metric_testing_metric_id(metric_key) %}
    {% set metric_name = 'Row count for ' ~ model_name %}

select
    cast('{{ metric_id }}' as {{ dbt.type_string() }}) as metric_id
  , cast('{{ metric_name }}' as {{ dbt.type_string() }}) as metric_name
  , cast(count(*) as {{ dbt.type_numeric() }}) as metric_value
from {{ ref(model_name) }}
{% endmacro %}


{% macro metric_testing_count_distinct_select(metric_group, model_name, column_name) %}
    {% set metric_key = metric_group ~ '__' ~ model_name ~ '__distinct_' ~ column_name ~ '_count' %}
    {% set metric_id = get_metric_testing_metric_id(metric_key) %}
    {% set metric_name = 'Distinct ' ~ column_name ~ ' count for ' ~ model_name %}

select
    cast('{{ metric_id }}' as {{ dbt.type_string() }}) as metric_id
  , cast('{{ metric_name }}' as {{ dbt.type_string() }}) as metric_name
  , cast(count(distinct {{ column_name }}) as {{ dbt.type_numeric() }}) as metric_value
from {{ ref(model_name) }}
{% endmacro %}


{% macro metric_testing_input_core_diff_select(suffix, input_model, core_model) %}
    {% set metric_key = 'input_to_core_diff__' ~ suffix ~ '__row_count' %}
    {% set metric_id = get_metric_testing_metric_id(metric_key) %}
    {% set metric_name = 'Row count diff for ' ~ suffix %}

select
    cast('{{ metric_id }}' as {{ dbt.type_string() }}) as metric_id
  , cast('{{ metric_name }}' as {{ dbt.type_string() }}) as metric_name
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
