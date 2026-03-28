{% macro clickhouse__regexp_instr(source_value, regexp, position, occurrence, is_raw, flags) %}
{# ClickHouse uses match() which returns UInt8 (1 = match, 0 = no match).
   The callers check regexp_instr(...) > 0, so returning 1/0 is compatible. #}
{% if is_raw or flags %}
    {{ exceptions.warn(
            "is_raw and flags options are not supported for ClickHouse "
            ~ "and are being ignored."
    ) }}
{% endif %}
multiIf(match({{ source_value }}, '{{ regexp }}'), 1, 0)
{% endmacro %}
