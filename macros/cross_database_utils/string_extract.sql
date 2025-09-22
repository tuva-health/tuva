{# --- This macro extracts text from a string using two modes 1) regex or 2) substring

    1) Regex mode (portable, prefer this when possible)
    Example: {{ string_extract('measure_name', pattern='\(([^)]+)\)', group=1) }} as measure_id
    Extract text inside parentheses. Ensure your pattern puts the desired text in group 1 for BigQuery/Redshift.

    2) Delimiter mode
    Example: {{ string_extract('measure_name', start_delim='(', end_delim=')') }} as measure_id

--- #}

{% macro string_extract(expr, pattern=None, start_delim=None, end_delim=None, group=1) %}
  {# --- Safety checks --- #}
  {% if pattern is none and (start_delim is none or end_delim is none) %}
    {{ exceptions.raise_compiler_error("string_extract: provide either `pattern` OR both `start_delim` and `end_delim`.") }}
  {% endif %}

  {# -------- Determine which version of the macro to dispatch -------- #}

  {# --- Regex path if a pattern is provided --- #}
  {% if pattern is not none %}
    {{ return(adapter.dispatch('string_extract__regex', 'the_tuva_project')(expr, pattern, group)) }}
  {# --- Delimiter (substring) path --- #}
  {% else %}
    {{ return(adapter.dispatch('string_extract__delims', 'the_tuva_project')(expr, start_delim, end_delim)) }}
  {% endif %}
{% endmacro %}

{# -------- Default to T-SQL -------- #}
{% macro default__string_extract__regex(expr, pattern, group) %}
  REGEXP_SUBSTR({{ expr }}, '{{ pattern }}', 1, 1, 'e', {{ group }})
{% endmacro %}

{# -------- BigQuery -------- #}
{# --- BigQuery returns the FIRST capture group; group>1 not selectable with REGEXP_EXTRACT. --- #}
{% macro bigquery__string_extract__regex(expr, pattern, group) %}
  {% if group != 1 %}
    {{ exceptions.warn("BigQuery: REGEXP_EXTRACT returns group 1 only. Ensure your `pattern` captures the desired text in group 1.") }}
  {% endif %}
  REGEXP_EXTRACT({{ expr }}, r'{{ pattern }}')
{% endmacro %}

{# -------- Fabric -------- #}
{% macro fabric__string_extract__regex(expr, pattern, group) %}
  regexp_extract({{ expr }}, '{{ pattern }}', {{ group }})
{% endmacro %}

{# -------- Redshift -------- #}
{# --- Emulates "return group 1" via a full-string replace to \1. Your pattern MUST define the desired text as ( ... ) in group 1. --- #}
{% macro redshift__string_extract__regex(expr, pattern, group) %}
  {% if group != 1 %}
    {{ exceptions.warn("Redshift: only capture group 1 is supported by this macro. Ensure your `pattern` captures the desired text in group 1.") }}
  {% endif %}
  REGEXP_REPLACE({{ expr }}, '.*({{ pattern }}).*', '\\1')
{% endmacro %}

{# -------- Snowflake -------- #}
{% macro snowflake__string_extract__regex(expr, pattern, group) %}
  REGEXP_SUBSTR({{ expr }}, '{{ pattern }}', 1, 1, 'e', {{ group }})
{% endmacro %}


{# --- Helper to raise if delimiters not found; most engines return NULL if any position is 0. --- #}

{# -------- Default to T-SQL -------- #}
{% macro default__string_extract__delims(expr, start_delim, end_delim) %}
  SUBSTRING(
    {{ expr }},
    CHARINDEX('{{ start_delim }}', {{ expr }}) + LENGTH('{{ start_delim }}'),
    CHARINDEX('{{ end_delim }}', {{ expr }})
      - CHARINDEX('{{ start_delim }}', {{ expr }})
      - LENGTH('{{ start_delim }}')
  )
{% endmacro %}

{# -------- BigQuery -------- #}
{% macro bigquery__string_extract__delims(expr, start_delim, end_delim) %}
  SUBSTR(
    {{ expr }},
    STRPOS({{ expr }}, '{{ start_delim }}') + LENGTH('{{ start_delim }}'),
    STRPOS({{ expr }}, '{{ end_delim }}')
      - STRPOS({{ expr }}, '{{ start_delim }}')
      - LENGTH('{{ start_delim }}')
  )
{% endmacro %}

{# -------- Fabric -------- #}
{% macro fabric__string_extract__delims(expr, start_delim, end_delim) %}
  SUBSTRING(
    {{ expr }},
    CHARINDEX('{{ start_delim }}', {{ expr }}) + LENGTH('{{ start_delim }}'),
    CHARINDEX('{{ end_delim }}', {{ expr }})
      - CHARINDEX('{{ start_delim }}', {{ expr }})
      - LENGTH('{{ start_delim }}')
  )
{% endmacro %}

{# -------- Redshift / Postgres -------- #}
{% macro redshift__string_extract__delims(expr, start_delim, end_delim) %}
  SUBSTRING(
    {{ expr }}
    FROM POSITION('{{ start_delim }}' IN {{ expr }}) + LENGTH('{{ start_delim }}')
    FOR  POSITION('{{ end_delim }}' IN {{ expr }})
       - POSITION('{{ start_delim }}' IN {{ expr }})
       - LENGTH('{{ start_delim }}')
  )
{% endmacro %}

{# -------- Snowflake -------- #}
{% macro snowflake__string_extract__delims(expr, start_delim, end_delim) %}
  SUBSTRING(
    {{ expr }},
    CHARINDEX('{{ start_delim }}', {{ expr }}) + LENGTH('{{ start_delim }}'),
    CHARINDEX('{{ end_delim }}', {{ expr }})
      - CHARINDEX('{{ start_delim }}', {{ expr }})
      - LENGTH('{{ start_delim }}')
  )
{% endmacro %}
