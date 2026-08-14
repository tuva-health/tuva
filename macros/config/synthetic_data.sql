{#
    Selects which Tuva synthetic data the Input Layer reads.

        vars:
          synthetic_data: small    the small payload
          synthetic_data: large    the large payload

    Leaving `synthetic_data` unset means none: the seeds stay disabled, so
    installing this package and running `dbt build` never loads them. Synthetic
    data is opt-in.

    Tuva synthetic data is the only thing this selects. To read an Input Layer
    you already have, point the source at it in
    integration_tests/models/_sources.yml -- none of this is involved.

    Seed YAML renders without macro context, so the same set-or-not check is
    written out inline in seeds/synthetic_data/synthetic_data_seeds.yml
    (enabled). Change one, change the other.
#}

{% macro tuva_synthetic_data_sizes() %}
  {{ return(['small', 'large']) }}
{% endmacro %}


{% macro tuva_synthetic_data_enabled() %}
  {% if (var('synthetic_data', '') or '') | string | trim == '' %}
    {{ return(false) }}
  {% endif %}
  {% do the_tuva_project.tuva_synthetic_data_size() %}
  {{ return(true) }}
{% endmacro %}


{% macro tuva_synthetic_data_size() %}
  {% set size = (var('synthetic_data', '') or '') | string | trim | lower %}
  {% if size not in the_tuva_project.tuva_synthetic_data_sizes() %}
    {% do exceptions.raise_compiler_error(
      "synthetic_data '" ~ size ~ "' is not valid. Use "
      ~ the_tuva_project.tuva_synthetic_data_sizes() | join(' or ')
      ~ ", or leave it unset to turn synthetic data off.") %}
  {% endif %}
  {{ return(size) }}
{% endmacro %}


{% macro tuva_synthetic_data_schema() %}
  {#- Repeated inline by the &schema anchor in synthetic_data_seeds.yml and by
      the source in integration_tests/models/_sources.yml, neither of which can
      call a macro. -#}
  {% set prefix = var('tuva_schema_prefix', none) %}
  {% if prefix is not none and prefix | trim != '' %}
    {{ return(prefix ~ '_synthetic_data') }}
  {% endif %}
  {{ return('synthetic_data') }}
{% endmacro %}


{% macro show_synthetic_data() %}
  {#- dbt run-operation show_synthetic_data -#}
  {% do log('', info=True) %}
  {% if not the_tuva_project.tuva_synthetic_data_enabled() %}
    {% do log('  synthetic_data: not set  (off)', info=True) %}
  {% else %}
    {% do log('  synthetic_data: ' ~ the_tuva_project.tuva_synthetic_data_size(), info=True) %}
    {% do log('  reads: ' ~ target.database ~ '.' ~ the_tuva_project.tuva_synthetic_data_schema(), info=True) %}
  {% endif %}
  {% do log('  valid: ' ~ the_tuva_project.tuva_synthetic_data_sizes() | join(', ') ~ ', or unset', info=True) %}
  {% do log('', info=True) %}
{% endmacro %}
