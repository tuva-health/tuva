{% macro is_fabric() %}
  {% if target.type == 'fabric' %}
    {{ return(true) }}
  {% else %}
    {{ return(false) }}
  {% endif %}
{% endmacro %}
