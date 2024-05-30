{% test has_any_data(model) %}

{{ config(severity = 'warn') }}

with onerow as (
    select 1 as has_data
    from {{ model }}
    limit 1
)

select
    case when count(has_data) > 0 then 'pass' else 'fail' end as has_any_data
from onerow

{% endtest %}