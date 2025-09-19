{{ config(
     tags = ['unit','hcc_suspecting','observation_suspects'],
     severity = 'ERROR',
     enabled = var('hcc_suspecting_enabled', var('claims_enabled', var('clinical_enabled', var('tuva_marts_enabled', False)))) | as_bool
) }}

-- Purpose:
-- Validate HCC suspect generation logic (HCC 48 & HCC 155) across happy paths, edge cases, and failure conditions.
-- Approach:
--  - Build ACTUAL from the same CTE chain as the target logic by reading the model under test (ref) directly.
--  - Define EXPECTED rows that MUST appear given the seeded/fixture data present in the repo.
--  - Use EXCEPT-based equality in both directions (symmetry) to catch missing/extra rows.
--
-- Notes:
--  - If your project uses ephemeral staging for the unit test, substitute `ref('hcc_suspecting__observation_suspects')`
--    with the appropriate final model that this test validates.
--  - This test assumes deterministic seed data and stable sorting for tie-breaking.

with actual as (
    -- If a dedicated output model exists, reference it here:
    -- select * from {{ ref('hcc_suspecting__observation_suspects') }}
    -- Fallback: reuse the "basic" test CTE chain to materialize the same shape.
    {{-
      return(
        adapter.dispatch('observation_suspects_actual', 'hcc_suspecting')()
      )
    -}}
),

-- Expected rows cover:
-- - HCC 48: BMI >= 40 (no condition); BMI >= 35 with diabetes; BMI >= 35 with hypertension; BMI >= 30 with OSA
-- - HCC 155: PHQ-9 highest of last 3 >= 15
-- - Negative controls: borderline values and mismatched-year condition/BMI combinations should not appear
expected as (
    select * from (values
        -- HCC 48: BMI >= 40 (no condition linkage required)
        -- person_id, data_source, observation_date, observation_result, condition_code, condition_date, condition_concept_name, hcc_code, hcc_description, current_year_billed, reason, contributing_factor, suspect_date
        ('P001','srcA',date '2024-06-15','40.0',null,null,null,'48','Morbid Obesity',false,'Observation suspect','BMI result 40.0',date '2024-06-15'),

        -- HCC 48: BMI >= 35 with diabetes in same year
        ('P002','srcA',date '2024-03-05','35.1','E11.9',date '2024-11-20','diabetes','48','Morbid Obesity',false,'Observation suspect','BMI result 35.1 with diabetes(E11.9 on 2024-11-20)',date '2024-03-05'),

        -- HCC 48: BMI >= 35 with essential hypertension in same year
        ('P003','srcB',date '2023-02-10','36.0','I10',date '2023-08-01','essential hypertension','48','Morbid Obesity',true,'Observation suspect','BMI result 36.0 with essential hypertension(I10 on 2023-08-01)',date '2023-02-10'),

        -- HCC 48: BMI >= 30 with OSA in same year
        ('P004','srcB',date '2025-01-30','33.3','G47.33',date '2025-05-15','obstructive sleep apnea','48','Morbid Obesity',false,'Observation suspect','BMI result 33.3 with obstructive sleep apnea(G47.33 on 2025-05-15)',date '2025-01-30'),

        -- HCC 155: PHQ-9 highest of last 3 assessments >= 15
        ('P005','srcC',date '2024-09-09','18',null,null,'depression assessment (phq-9)','155','Major Depression, Moderate or Severe, without Psychosis',false,'Observation suspect','PHQ-9 result 18 on 2024-09-09',date '2024-09-09')
    )
    as t(
        person_id, data_source, observation_date, observation_result,
        condition_code, condition_date, condition_concept_name,
        hcc_code, hcc_description, current_year_billed, reason, contributing_factor, suspect_date
    )
),

-- Fail if ACTUAL has rows not listed as EXPECTED for our fixture set.
unexpected as (
    select * from actual
    except
    select * from expected
),

-- Fail if EXPECTED rows are missing from ACTUAL.
missing as (
    select * from expected
    except
    select * from actual
)

select * from unexpected
union all
select * from missing
;