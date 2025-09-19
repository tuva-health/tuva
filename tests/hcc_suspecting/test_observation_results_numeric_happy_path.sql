{{ config(
    tags=['unit','hcc_suspecting','numeric_observations'],
    severity='error'
) }}

-- Testing library/framework: dbt native data tests (dbt test).
-- Purpose: Ensure rows with strictly numeric results are included by the numeric_observations CTE logic.

with observations as (
    select * from {{ ref('hcc_suspecting__stg_core__observation') }}
),
numeric_filtered as (
    select
        person_id, observation_date, result, code_type, code, data_source
    from observations
    {% if target.type == 'fabric' %}
        where result like '%.%' or result like '%[0-9]%'
        and result not like '%[^0-9.]%'
    {% else %}
        where {{ apply_regex('result', '^[+-]?([0-9]*[.])?[0-9]+$') }}
    {% endif %}
)
select *
from numeric_filtered
where person_id in (1,2,4,5,6,7,11,14,15)
-- Expected: these have numeric-like values.
-- The test passes if this returns at least those rows. To adhere to dbt data test (0 rows pass), we invert the expectation:
-- We assert there are no false negatives: for each expected numeric person_id, ensure they are present.
-- Construct a set of required ids and left anti-join.

-- 0-rows check: any missing expected ids indicates a failure
with expected as (select * from (values (1),(2),(4),(5),(6),(7),(11),(14),(15)) as t(person_id)),
found as (select distinct person_id from numeric_filtered)
select e.person_id from expected e left join found f on e.person_id = f.person_id where f.person_id is null