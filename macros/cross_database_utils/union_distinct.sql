{% macro union_distinct() %}
{#
    Emits the set-union keyword that de-duplicates rows.

    Every adapter this package targets spells it `union distinct` except Fabric,
    where the T-SQL `UNION` already de-duplicates and `UNION DISTINCT` is not
    valid syntax.

    Written inline in eighteen places before this macro existed, which meant an
    adapter that spelled it a third way had to be fixed eighteen times. Add the
    branch here instead.

        select ... {{ union_distinct() }} select ...
#}
{%- if target.type == 'fabric' -%}
union
{%- else -%}
union distinct
{%- endif -%}
{% endmacro %}
