{% test valid_values(model, column_name, lookup_table, lookup_column) %}

with validation as (
select
    {{ column_name }} as col
from {{ model }}
),

validation_errors as (
select
    col
from validation aa
left join {{ lookup_table }} bb
    on aa.col = bb.{{ lookup_column }}
where bb.{{ lookup_column }} is null
)

select *
from validation_errors

{% endtest %}