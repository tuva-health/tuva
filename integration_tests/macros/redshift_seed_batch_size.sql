{% macro redshift__get_batch_size() %}
  {{ return(env_var('DBT_REDSHIFT_CI_SEED_BATCH_SIZE', '2000') | int) }}
{% endmacro %}
