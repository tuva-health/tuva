{{ config(
    tags=['unit','hcc_suspecting','numeric_observations'],
    severity='error'
) }}

-- Testing library/framework: dbt native data tests (dbt test).
-- Purpose: Validate regex edge cases for numeric detection.

with observations as (
    select * from {{ ref('hcc_suspecting__stg_core__observation') }}
),
numeric_filtered as (
    select person_id, result
    from observations
    {% if target.type == 'fabric' %}
        -- Fabric path uses LIKE patterns which may treat '75.' as numeric.
        where (result like '%.%' or result like '%[0-9]%')
          and result not like '%[^0-9.]%'
    {% else %}
        -- Regex path: 75. should be invalid (requires digits after dot).
        where {{ apply_regex('result', '^[+-]?([0-9]*[.])?[0-9]+$') }}
    {% endif %}
)
-- Assert that .75 (7) and +12.7 (6) and -15 (5) are included; 75. (8) excluded for non-fabric.
-- Build 0-rows failure query for missing expected inclusions and unexpected inclusions.

, expected_inclusions as (
  select * from (values (5), (6), (7)) as t(person_id)
),
found as (
  select distinct person_id from numeric_filtered
),
missing as (
  select e.person_id from expected_inclusions e
  left join found f on e.person_id = f.person_id
  where f.person_id is null
),
unexpected as (
  select f.person_id 
  from found f
  where f.person_id in (8) -- 75. should be excluded on non-fabric
  {% if target.type == 'fabric' %}
    and 1=0 -- allow on fabric
  {% endif %}
)
select * from missing
union all
select * from unexpected
-- Expect 0 rows