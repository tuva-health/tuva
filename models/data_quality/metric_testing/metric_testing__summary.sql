{{ config(
     enabled = var('metric_testing_enabled', False) | as_bool
   )
}}

{% set input_models = get_metric_testing_models('input_layer') %}
{% set core_models = get_metric_testing_models('core_final') %}
{% set paired_models = get_metric_testing_input_core_pairs() %}
{% set data_mart_models = get_metric_testing_models('data_mart_summary') %}
{% set selects = [] %}

{% for model_name in input_models %}
    {% do selects.append(metric_testing_row_count_select('input_layer', model_name)) %}
{% endfor %}

{% for model_name in core_models %}
    {% do selects.append(metric_testing_row_count_select('core', model_name)) %}
{% endfor %}

{% for pair in paired_models %}
    {% do selects.append(metric_testing_input_core_diff_select(pair['suffix'], pair['input_model'], pair['core_model'])) %}
{% endfor %}

{% for model_name in data_mart_models %}
    {% do selects.append(metric_testing_row_count_select('data_mart', model_name)) %}
{% endfor %}

{% if 'core__medical_claim' in core_models %}
    {% do selects.append(metric_testing_count_distinct_select('core', 'core__medical_claim', 'claim_id')) %}
{% endif %}

{% if selects | length == 0 %}
    {{ metric_testing_empty_result() }}
{% else %}
    {{ selects | join('\n\nunion all\n\n') }}
{% endif %}
