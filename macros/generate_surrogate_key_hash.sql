{% macro generate_surrogate_key_hash(columns) %}
{% if target.type == 'snowflake' %}
    -- Snowflake-specific logic for generating a surrogate key
    TO_NUMBER(
        SUBSTRING(
            MD5(CONCAT(
                {% for column in columns %}
                    COALESCE(CAST({{ column }} AS VARCHAR), '') || '-'
                {% endfor %}
                ''
            )),
            1, 15
        ),
        'XXXXXXXXXXXXXXX'
    ) AS sk
{% endif %}
{% endmacro %}