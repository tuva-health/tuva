{% macro get_cli_command() %}
  {% set invocation_command = invocation_args_dict.get('which', 'unknown') %}
  {% set full_command_parts = ['dbt', invocation_command] %}

  {# Add target if specified #}
  {% if target.name != 'default' %}
    {% do full_command_parts.append('--target') %}
    {% do full_command_parts.append(target.name) %}
  {% endif %}

  {# Add profiles-dir if specified #}
  {% if invocation_args_dict.get('profiles_dir') %}
    {% do full_command_parts.append('--profiles-dir') %}
    {% do full_command_parts.append(invocation_args_dict.get('profiles_dir')) %}
  {% endif %}

  {# Add project-dir if specified #}
  {% if invocation_args_dict.get('project_dir') %}
    {% do full_command_parts.append('--project-dir') %}
    {% do full_command_parts.append(invocation_args_dict.get('project_dir')) %}
  {% endif %}

  {# Add vars if specified #}
  {% if invocation_args_dict.get('vars') %}
    {% do full_command_parts.append('--vars') %}
    {% do full_command_parts.append("'" ~ invocation_args_dict.get('vars') ~ "'") %}
  {% endif %}

  {# Add select if specified #}
  {% if invocation_args_dict.get('select') %}
    {% do full_command_parts.append('--select') %}
    {% do full_command_parts.append(invocation_args_dict.get('select')) %}
  {% endif %}

  {# Add exclude if specified #}
  {% if invocation_args_dict.get('exclude') %}
    {% do full_command_parts.append('--exclude') %}
    {% do full_command_parts.append(invocation_args_dict.get('exclude')) %}
  {% endif %}

  {# Add models if specified (for legacy compatibility) #}
  {% if invocation_args_dict.get('models') %}
    {% do full_command_parts.append('--models') %}
    {% do full_command_parts.append(invocation_args_dict.get('models')) %}
  {% endif %}

  {# Add full-refresh if specified #}
  {% if invocation_args_dict.get('full_refresh') %}
    {% do full_command_parts.append('--full-refresh') %}
  {% endif %}

  {# Add threads if specified #}
  {% if invocation_args_dict.get('threads') %}
    {% do full_command_parts.append('--threads') %}
    {% do full_command_parts.append(invocation_args_dict.get('threads')) %}
  {% endif %}

  {# Join all parts with spaces #}
  {% set full_command = full_command_parts | join(' ') %}

  {{ return(full_command) }}
{% endmacro %}


{# Helper macro to print the command during execution #}
{% macro print_cli_command() %}
  {% set cli_command = get_cli_command() %}
  {{ log("Full CLI command: " ~ cli_command, info=true) }}
  {{ return(cli_command) }}
{% endmacro %}


{# Macro to get command as a column value in a model #}
{% macro cli_command_column() %}
  '{{ get_cli_command() | replace("'", "''") }}'
{% endmacro %}