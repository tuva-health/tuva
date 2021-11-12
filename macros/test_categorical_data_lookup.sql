{% test categorical_data_lookup(model, column_name, code_set) %}

select
    {{column_name}}
from {{model}} a
left join categorical_data b
    on a.{{column_name}} = b.code_value
    and b.code_set = '{{code_set}}'
where b.code_value is null

{% endtest %}