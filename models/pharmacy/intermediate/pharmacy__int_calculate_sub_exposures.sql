{{ config(
    enabled = var('brand_generic_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
) }}

with drug_exposure_input as (
    select * from {{ ref('pharmacy__stg_add_ingredient_concepts') }}
),

-- Step 1: Detect gaps using simple window functions
exposures_with_gaps as (
    select
        person_id,
        ingredient_rxcui,
        ingredient_name,
        drug_exposure_start_date,
        drug_exposure_end_date,
        days_supply,
        -- Get previous exposure's end date for gap detection
        lag(drug_exposure_end_date) over(
            partition by person_id, ingredient_rxcui 
            order by drug_exposure_start_date, drug_exposure_end_date
        ) as prev_end_date
    from drug_exposure_input
),

-- Step 2: Assign group IDs based on gaps
exposures_with_groups as (
    select
        person_id,
        ingredient_rxcui,
        ingredient_name,
        drug_exposure_start_date,
        drug_exposure_end_date,
        -- Create group IDs: increment when there's a gap >= 1 day
        -- A gap exists when current start > previous end + 1 day
        sum(case
            when prev_end_date is null then 1  -- First record starts a new group
            when drug_exposure_start_date > {{ dbt.dateadd('day', 1, 'prev_end_date') }} then 1  -- Gap detected
            else 0  -- Continuous coverage
        end) over(
            partition by person_id, ingredient_rxcui
            order by drug_exposure_start_date, drug_exposure_end_date
            rows between unbounded preceding and current row
        ) as group_id
    from exposures_with_gaps
)

-- Step 3: Aggregate by group to create sub-exposures
select
    person_id,
    ingredient_rxcui,
    ingredient_name,
    min(drug_exposure_start_date) as drug_sub_exposure_start_date,
    max(drug_exposure_end_date) as drug_sub_exposure_end_date,
    count(*) as drug_exposure_count,
    {{ dbt.datediff('min(drug_exposure_start_date)', 'max(drug_exposure_end_date)', 'day') }} + 1 as days_exposed
from exposures_with_groups
group by
    person_id,
    ingredient_rxcui,
    ingredient_name,
    group_id
