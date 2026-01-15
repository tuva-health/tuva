{{ config(
    enabled = var('brand_generic_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
) }}

with pharmacy_claim_input as (
    select * from {{ ref('pharmacy_claim') }}
),

product_to_ingredient as (
    select * from {{ ref('pharmacy__product_to_ingredient') }}
),

-- Step 1: Prepare claim lines with calculated end dates
prepared_claim_lines as (
    select
        claim_id,
        claim_line_number,
        data_source,
        person_id,
        ndc_code,
        dispensing_date,
        days_supply,
        -- Calculate original end date at claim line level
        case 
            when days_supply > 0 
            then {{ dbt.dateadd('day', 'days_supply - 1', 'dispensing_date') }}
            else dispensing_date 
        end as original_end_date
    from pharmacy_claim_input
    -- Filter invalid data upfront for data quality
    where days_supply is not null 
      and days_supply > 0
),

-- Step 2: Explode claim lines to ingredient level
claim_line_ingredients as (
    select
        pcl.claim_id,
        pcl.claim_line_number,
        pcl.data_source,
        pcl.person_id,
        pcl.ndc_code,
        pcl.dispensing_date as original_start_date,
        pcl.original_end_date,
        pcl.days_supply,
        pti.ingredient_rxcui,
        pti.ingredient_name
    from prepared_claim_lines pcl
    inner join product_to_ingredient pti
        on pcl.ndc_code = pti.ndc
),

-- Step 3: Work at claim_line level (not claim level) to preserve granularity
-- Each claim line may have different dates, so we treat them independently
unique_claim_lines as (
    select distinct
        claim_id,
        claim_line_number,
        data_source,
        person_id,
        original_start_date,
        original_end_date,
        days_supply
    from claim_line_ingredients
),

-- Step 4: Get all ingredients for each claim line
claim_line_ingredients_distinct as (
    select distinct
        claim_id,
        claim_line_number,
        data_source,
        person_id,
        ingredient_rxcui,
        ingredient_name
    from claim_line_ingredients
),

-- Step 5: Replaced complex self-join and recursive CTE logic to use LAG to find the prior exposure for each ingredient
ingredient_with_prior as (
    select
        cli.claim_id,
        cli.claim_line_number,
        cli.data_source,
        cli.person_id,
        cli.ingredient_rxcui,
        cli.ingredient_name,
        ucl.original_start_date,
        ucl.original_end_date,
        ucl.days_supply,
        lag(ucl.original_end_date) over (
            partition by cli.person_id, cli.ingredient_rxcui
            order by ucl.original_start_date, cli.claim_id, cli.claim_line_number
        ) as prior_end_date
    from claim_line_ingredients_distinct cli
    inner join unique_claim_lines ucl
        on cli.claim_id = ucl.claim_id
        and cli.claim_line_number = ucl.claim_line_number
        and cli.data_source = ucl.data_source
        and cli.person_id = ucl.person_id
),

-- Step 6: For each claim line, find the latest prior_end_date across all its ingredients
-- This ensures all ingredients from the same claim line stay synchronized
claim_line_dependencies as (
    select
        claim_id,
        claim_line_number,
        data_source,
        person_id,
        original_start_date,
        original_end_date,
        days_supply,
        max(prior_end_date) as max_prior_end_date
    from ingredient_with_prior
    group by
        claim_id,
        claim_line_number,
        data_source,
        person_id,
        original_start_date,
        original_end_date,
        days_supply
),

-- Step 7: Adjust claim lines based on prior exposures
adjusted_claim_lines as (
    select
        claim_id,
        claim_line_number,
        data_source,
        person_id,
        original_start_date,
        original_end_date,
        days_supply,
        -- Adjusted start: later of (original start, prior end + 1 day)
        case
            when max_prior_end_date is not null
            then greatest(
                original_start_date,
                {{ dbt.dateadd('day', 1, 'max_prior_end_date') }}
            )
            else original_start_date
        end as adjusted_start_date
    from claim_line_dependencies
),

-- Step 8: Calculate adjusted end date for each claim line
adjusted_claim_lines_with_end as (
    select
        claim_id,
        claim_line_number,
        data_source,
        person_id,
        adjusted_start_date,
        -- Adjusted end: adjusted start + days_supply - 1
        {{ dbt.dateadd('day', 'days_supply - 1', 'adjusted_start_date') }} as adjusted_end_date,
        days_supply
    from adjusted_claim_lines
),

-- Step 9: Join adjusted dates back to all claim line ingredients
-- All ingredients from the same claim line get the same adjusted dates
final_ingredient_exposures as (
    select
        cli.claim_id,
        cli.claim_line_number,
        cli.data_source,
        cli.person_id,
        cli.ingredient_rxcui,
        cli.ingredient_name,
        ac.adjusted_start_date as drug_exposure_start_date,
        ac.adjusted_end_date as drug_exposure_end_date,
        cli.days_supply
    from claim_line_ingredients cli
    inner join adjusted_claim_lines_with_end ac
        on cli.claim_id = ac.claim_id
        and cli.claim_line_number = ac.claim_line_number
        and cli.data_source = ac.data_source
        and cli.person_id = ac.person_id
)

select * from final_ingredient_exposures
