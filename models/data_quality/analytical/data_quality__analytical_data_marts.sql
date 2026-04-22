{{ config(
     enabled = var('enable_data_quality', false) | as_bool,
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'analytical_data_marts',
     tags = ['data_quality', 'dq', 'dq2', 'dq_analytics', 'dq_analytical'],
     materialized = 'table'
   )
}}

{% set analytical_data_mart_dependency_names = [] %}

{% if var('claims_enabled', false) | as_bool %}
  {% do analytical_data_mart_dependency_names.extend([
      'ahrq_measures__pqi_summary',
      'ccsr__procedure_summary',
      'chronic_conditions__cms_chronic_conditions_wide',
      'cms_hcc__patient_risk_scores',
      'ed_classification__summary',
      'financial_pmpm__pmpm_payer',
      'hcc_recapture__recapture_rates',
      'hcc_suspecting__summary',
      'pharmacy__brand_generic_opportunity',
      'quality_measures__summary_wide',
      'readmissions__readmission_summary'
  ]) %}
{% endif %}

{% if var('provider_attribution_enabled', false) | as_bool %}
  {% do analytical_data_mart_dependency_names.append('provider_attribution__provider_ranking') %}
{% endif %}

{% if var('semantic_layer_enabled', false) | as_bool %}
  {% do analytical_data_mart_dependency_names.extend([
      'semantic_layer__fact_member_months',
      'semantic_layer__fact_quality_measures'
  ]) %}
{% endif %}

{% for dependency_name in analytical_data_mart_dependency_names %}
-- depends_on: {{ ref(dependency_name) }}
{% endfor %}

{% if execute %}
    {% set final_marts = dq_representative_data_marts() %}
    {% set mart_queries = [] %}

    {% for final_mart in final_marts %}
        {% set model_node = final_mart['node'] %}
        {% set relation = dq_actual_relation(model_node) %}

        {% if relation is none %}
            {% set query %}
                select
                      '{{ final_mart['data_mart_name'] }}' as data_mart
                    , cast(null as {{ dbt.type_int() }}) as row_count
            {% endset %}
        {% else %}
            {% set query %}
                select
                      '{{ final_mart['data_mart_name'] }}' as data_mart
                    , cast(count(*) as {{ dbt.type_int() }}) as row_count
                from {{ relation }}
            {% endset %}
        {% endif %}

        {% do mart_queries.append(query) %}
    {% endfor %}

    select *
    from (
        {{ mart_queries | join('\nunion all\n') }}
    ) as mart_counts
    order by 1
{% else %}
    select
          cast(null as {{ dbt.type_string() }}) as data_mart
        , cast(null as {{ dbt.type_int() }}) as row_count
    where 1 = 0
{% endif %}
