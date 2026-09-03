{% macro dq_current_date_sql() %}
    {{ return("cast(" ~ dbt.current_timestamp() ~ " as " ~ api.Column.translate_type('date') ~ ")") }}
{% endmacro %}

{% macro dq_date_literal_sql(date_string) %}
    {{ return(dbt.cast("'" ~ date_string ~ "'", api.Column.translate_type('date'))) }}
{% endmacro %}

{% macro dq_date_in_range_where_sql(date_sql, minimum_date_sql, maximum_date_sql) %}
    {{ return(
        "(" ~ date_sql ~ " >= " ~ minimum_date_sql
        ~ " and " ~ date_sql ~ " <= " ~ maximum_date_sql ~ ")"
    ) }}
{% endmacro %}

{% macro dq_member_month_spine_date_where_sql(date_sql) %}
    {{ return(dq_date_in_range_where_sql(
        date_sql,
        dq_date_literal_sql('1900-01-01'),
        dq_date_literal_sql('2100-12-31')
    )) }}
{% endmacro %}

{% macro dq_logical_date_range_flag_sql(date_sql, minimum_date_sql, maximum_date_sql, applicability_sql=none) %}
    {% set effective_applicability_sql = applicability_sql if applicability_sql is not none else date_sql ~ " is not null" %}

    {{ return(dq_logical_int_flag_sql(
        "not " ~ dq_date_in_range_where_sql(date_sql, minimum_date_sql, maximum_date_sql),
        effective_applicability_sql
    )) }}
{% endmacro %}

{% macro dq_logical_ingest_datetime_range_flag_sql(timestamp_sql) %}
    {% set date_type = api.Column.translate_type('date') %}
    {% set ingest_date_sql = "cast(" ~ timestamp_sql ~ " as " ~ date_type ~ ")" %}

    {{ return(dq_logical_date_range_flag_sql(
        ingest_date_sql,
        dq_date_literal_sql('2000-01-01'),
        dq_current_date_sql(),
        timestamp_sql ~ " is not null"
    )) }}
{% endmacro %}

{% macro dq_digits_only_sql(expression) %}
    {% set digits_removed = namespace(expression=expression) %}

    {% for digit in ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9'] %}
        {% set digits_removed.expression = "replace(" ~ digits_removed.expression ~ ", '" ~ digit ~ "', '')" %}
    {% endfor %}

    {% set sentinel_wrapped_expression = concat_custom([
        digits_removed.expression,
        "'|'"
    ]) %}

    {{ return("(" ~ length(sentinel_wrapped_expression) ~ " = 1)") }}
{% endmacro %}

{% macro dq_has_any_columns_populated_sql(column_names, relation_alias='source_rows') %}
    {% set clauses = [] %}

    {% for column_name in column_names %}
        {% do clauses.append(relation_alias ~ "." ~ quote_column(column_name) ~ " is not null") %}
    {% endfor %}

    {{ return("(" ~ clauses | join(" or ") ~ ")") }}
{% endmacro %}

{% macro dq_medical_claim_inpatient_facility_where_sql(relation_alias='source_rows') %}
    {% set bill_type_prefix_expression = substring("cast(" ~ relation_alias ~ ".bill_type_code as " ~ dbt.type_string() ~ ")", 1, 2) %}

    {{ return(
        "lower(cast(" ~ relation_alias ~ ".claim_type as " ~ dbt.type_string() ~ ")) = 'institutional'"
        ~ " and " ~ relation_alias ~ ".bill_type_code is not null"
        ~ " and " ~ bill_type_prefix_expression ~ " in ('11', '12', '15', '16', '17', '18', '21', '22', '25', '26', '27', '28', '31', '41', '42', '45', '46', '47', '48', '61', '62', '65', '66', '67', '68', '82')"
    ) }}
{% endmacro %}

{% macro dq_medical_claim_acute_inpatient_where_sql(relation_alias='source_rows') %}
    {% set bill_type_prefix_expression = substring("cast(" ~ relation_alias ~ ".bill_type_code as " ~ dbt.type_string() ~ ")", 1, 2) %}

    {{ return(
        "lower(cast(" ~ relation_alias ~ ".claim_type as " ~ dbt.type_string() ~ ")) = 'institutional'"
        ~ " and " ~ relation_alias ~ ".bill_type_code is not null"
        ~ " and " ~ bill_type_prefix_expression ~ " in ('11', '12')"
    ) }}
{% endmacro %}


{#
    Athena caps a query string at 262,144 bytes. The logical failure-key and
    result models emit one UNION ALL branch per enabled logical test, which
    passes that cap well before the manifest is exhausted, and unlike the test
    catalog they cannot be brought under it by trimming aliases and whitespace.

    Splitting the manifest across several materialized part models turns one
    oversized statement into several small ones. The public relation stays a
    thin UNION over the parts. Parts are implementation details, not consumer
    contracts.
#}

{% macro dq_logical_chunk_count() %}
    {{ return(var('dq_logical_chunk_count', 4) | int) }}
{% endmacro %}


{% macro dq_enabled_logical_test_manifest_chunk(chunk_index) %}
    {% set chunk_count = the_tuva_project.dq_logical_chunk_count() %}
    {% set chunk = [] %}
    {% for definition in dq_enabled_logical_test_manifest() %}
        {% if loop.index0 % chunk_count == chunk_index %}
            {% do chunk.append(definition) %}
        {% endif %}
    {% endfor %}
    {{ return(chunk) }}
{% endmacro %}


{#
    logical_test_results groups the manifest by source flag model and builds
    per-model aggregate CTEs, so its chunks must split on whole models. Slicing
    on definition index would spread one flag model's definitions across parts
    and each part would aggregate only its own slice.
#}

{% macro dq_enabled_logical_test_manifest_chunk_by_model(chunk_index) %}
    {% set chunk_count = the_tuva_project.dq_logical_chunk_count() %}
    {% set source_models = [] %}
    {% for definition in dq_enabled_logical_test_manifest() %}
        {% if definition['source_model_name'] not in source_models %}
            {% do source_models.append(definition['source_model_name']) %}
        {% endif %}
    {% endfor %}
    {% set selected = [] %}
    {% for source_model_name in source_models %}
        {% if loop.index0 % chunk_count == chunk_index %}
            {% do selected.append(source_model_name) %}
        {% endif %}
    {% endfor %}
    {% set chunk = [] %}
    {% for definition in dq_enabled_logical_test_manifest() %}
        {% if definition['source_model_name'] in selected %}
            {% do chunk.append(definition) %}
        {% endif %}
    {% endfor %}
    {{ return(chunk) }}
{% endmacro %}
