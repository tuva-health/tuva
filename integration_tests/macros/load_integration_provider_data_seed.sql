{% macro redshift__load_provider_data_seed(pattern) %}
    {#
      Redshift COPY is unusually slow on the full provider-data release files during the
      default small synthetic integration build. For that narrow case, keep the checked-in
      reduced provider_data seed subsets and skip the versioned S3 reload.
    #}
    {% if target.type == 'redshift' and the_tuva_project.get_synthetic_data_size() == 'small' %}
        {{ return('') }}
    {% endif %}

    {{ return(the_tuva_project.load_versioned_seed('provider_data', pattern)) }}
{% endmacro %}
