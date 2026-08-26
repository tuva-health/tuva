{% macro dq_current_date_sql() %}
    {{ return("cast(" ~ dbt.current_timestamp() ~ " as " ~ api.Column.translate_type('date') ~ ")") }}
{% endmacro %}

{% macro dq_date_literal_sql(date_string) %}
    {{ return(dbt.cast("'" ~ date_string ~ "'", api.Column.translate_type('date'))) }}
{% endmacro %}

{% macro dq_logical_ingest_datetime_range_flag_sql(timestamp_sql) %}
    {% set date_type = api.Column.translate_type('date') %}
    {% set ingest_date_sql = "cast(" ~ timestamp_sql ~ " as " ~ date_type ~ ")" %}
    {% set minimum_date_sql = dq_date_literal_sql('2000-01-01') %}
    {% set maximum_date_sql = dq_current_date_sql() %}

    {{ return(dq_logical_int_flag_sql(
        ingest_date_sql ~ " < " ~ minimum_date_sql ~ " or " ~ ingest_date_sql ~ " > " ~ maximum_date_sql,
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
