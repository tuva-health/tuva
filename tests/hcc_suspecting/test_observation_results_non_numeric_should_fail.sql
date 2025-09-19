{{ config(
    tags=['unit','hcc_suspecting','numeric_observations'],
    severity='error',
    enabled=var('enable_negative_case_tests', true)
) }}

-- Testing library/framework: dbt native data tests (dbt test).
-- Purpose: Ensure clearly non-numeric results are excluded by the numeric filter.

with observations as (
    select * from {{ ref('hcc_suspecting__stg_core__observation') }}
),
numeric_filtered as (
    select
        person_id, result
    from observations
    {% if target.type == 'fabric' %}
        where result like '%.%' or result like '%[0-9]%'
        and result not like '%[^0-9.]%'
    {% else %}
        where {{ apply_regex('result', '^[+-]?([0-9]*[.])?[0-9]+$') }}
    {% endif %}
)
select *
from observations o
left join numeric_filtered n on o.person_id = n.person_id
where o.person_id in (3,8,9,10,12,13)
  and n.person_id is not null
-- 0 rows expected (any returned row indicates a failure: a non-numeric value slipped through)