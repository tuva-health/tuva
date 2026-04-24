{% macro tuva_source(table_name) %}
  {%- if var('use_synthetic_data', false) | as_bool -%}
    {%- do return(ref('raw_data__' ~ table_name)) -%}
  {%- else -%}
    {%- do return(source('source_input', table_name)) -%}
  {%- endif -%}
{% endmacro %}
