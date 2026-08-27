{% macro tuva_boolean_var(name, default=false) %}
    {% set value = var(name, default) %}

    {% if value is not sameas true and value is not sameas false %}
        {% do exceptions.raise_compiler_error(
            "Tuva variable '" ~ name ~ "' must be a native boolean "
            ~ "(unquoted true or false in YAML). Received: '" ~ value ~ "'."
        ) %}
    {% endif %}

    {% do return(value) %}
{% endmacro %}
