{{ config(
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'analytical_data_marts',
     tags = ['data_quality', 'dqi'],
     materialized = 'table'
   )
}}

{% set analytical_data_mart_dependency_names = [
    'ahrq_measures__pqi_summary',
    'ccsr__procedure_summary',
    'chronic_conditions__cms_chronic_conditions_wide',
    'cms_hcc__patient_risk_scores',
    'ed_classification__summary',
    'financial_pmpm__pmpm_payer',
    'hcc_recapture__recapture_rates',
    'hcc_suspecting__summary',
    'pharmacy__brand_generic_opportunity',
    'provider_attribution__provider_ranking',
    'quality_measures__summary_wide',
    'readmissions__readmission_summary',
    'semantic_layer__fact_member_months',
    'semantic_layer__fact_quality_measures'
] %}

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
                      cast('all' as {{ dbt.type_string() }}) as data_source
                    , '{{ final_mart['data_mart_name'] }}' as data_mart_name
                    , cast(null as {{ dbt.type_numeric() }}) as row_count
            {% endset %}
        {% else %}
            {% set actual_columns = dq_actual_columns(relation) %}

            {% if dq_has_column(actual_columns, 'data_source') %}
                {% set query %}
                    select
                          mart_counts.data_source
                        , '{{ final_mart['data_mart_name'] }}' as data_mart_name
                        , cast(mart_counts.row_count as {{ dbt.type_numeric() }}) as row_count
                    from (
                        {{ dq_grouped_rowcount_sql(relation, ['data_source']) }}
                    ) as mart_counts

                    union all

                    select
                          cast('all' as {{ dbt.type_string() }}) as data_source
                        , '{{ final_mart['data_mart_name'] }}' as data_mart_name
                        , cast(0 as {{ dbt.type_numeric() }}) as row_count
                    where not exists (
                        select 1
                        from {{ relation }}
                    )
                {% endset %}
            {% else %}
                {% set query %}
                    select
                          cast('all' as {{ dbt.type_string() }}) as data_source
                        , '{{ final_mart['data_mart_name'] }}' as data_mart_name
                        , cast(count(*) as {{ dbt.type_numeric() }}) as row_count
                    from {{ relation }}
                {% endset %}
            {% endif %}
        {% endif %}

        {% do mart_queries.append(query) %}
    {% endfor %}

    {{ mart_queries | join('\nunion all\n') }}
{% else %}
    select
          cast(null as {{ dbt.type_string() }}) as data_source
        , cast(null as {{ dbt.type_string() }}) as data_mart_name
        , cast(null as {{ dbt.type_numeric() }}) as row_count
    where 1 = 0
{% endif %}
