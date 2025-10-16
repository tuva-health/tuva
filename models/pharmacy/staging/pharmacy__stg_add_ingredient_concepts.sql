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

-- Step 2: Explode claim lines to ingredient level BEFORE adjustment
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

-- Step 3: Number rows at the ingredient level for sequential processing
numbered_ingredient_exposures as (
    select
        claim_id,
        claim_line_number,
        data_source,
        person_id,
        ndc_code,
        original_start_date,
        original_end_date,
        days_supply,
        ingredient_rxcui,
        ingredient_name,
        row_number() over(
            partition by person_id, ingredient_rxcui  -- Partition by individual ingredient
            order by original_start_date, claim_id, claim_line_number, data_source
        ) as rn
    from claim_line_ingredients
),

-- Step 4: RECURSIVE ADJUSTMENT AT INGREDIENT LEVEL
-- Each ingredient's chronology is adjusted independently
recursive_ingredient_adjustments as (
    -- Base case: first exposure for each person + ingredient combination
    select
        claim_id,
        claim_line_number,
        data_source,
        person_id,
        ndc_code,
        original_start_date,
        original_end_date,
        days_supply,
        ingredient_rxcui,
        ingredient_name,
        rn,
        original_start_date as adjusted_start_date,
        original_end_date as adjusted_end_date
    from numbered_ingredient_exposures
    where rn = 1
    
    union all
    
    -- Recursive case: subsequent exposures for same ingredient
    select
        nie.claim_id,
        nie.claim_line_number,
        nie.data_source,
        nie.person_id,
        nie.ndc_code,
        nie.original_start_date,
        nie.original_end_date,
        nie.days_supply,
        nie.ingredient_rxcui,
        nie.ingredient_name,
        nie.rn,
        -- Adjusted start: later of (original start, prior adjusted end + 1 day)
        greatest(
            nie.original_start_date,
            {{ dbt.dateadd('day', 1, 'ria.adjusted_end_date') }}
        ) as adjusted_start_date,
        -- Adjusted end: adjusted start + days_supply - 1
        {{ dbt.dateadd(
            'day',
            'nie.days_supply - 1',
            'greatest(
                nie.original_start_date,
                ' ~ dbt.dateadd('day', 1, 'ria.adjusted_end_date') ~ '
            )'
        ) }} as adjusted_end_date
    from numbered_ingredient_exposures nie
    inner join recursive_ingredient_adjustments ria
        on nie.person_id = ria.person_id
        and nie.ingredient_rxcui = ria.ingredient_rxcui  -- Match on specific ingredient
        and nie.rn = ria.rn + 1  -- Sequential within ingredient
),

-- Step 5: Collapse back to claim line level
-- Take the MAX adjusted start date across all ingredients for each claim line
claim_line_adjustments as (
    select
        claim_id,
        claim_line_number,
        data_source,
        person_id,
        ndc_code,
        max(adjusted_start_date) as adjusted_start_date,
        -- Get days_supply (same for all ingredients from same claim line)
        max(days_supply) as days_supply
    from recursive_ingredient_adjustments
    group by
        claim_id,
        claim_line_number,
        data_source,
        person_id,
        ndc_code
),

-- Step 6: Calculate synchronized end date for the claim line
claim_line_with_end_dates as (
    select
        claim_id,
        claim_line_number,
        data_source,
        person_id,
        ndc_code,
        adjusted_start_date,
        -- Recompute end date based on the synchronized start date
        {{ dbt.dateadd('day', 'days_supply - 1', 'adjusted_start_date') }} as adjusted_end_date,
        days_supply
    from claim_line_adjustments
),

-- Step 7: Join synchronized dates back to all ingredients from each claim
-- This ensures all ingredients from the same claim have identical coverage periods
final_ingredient_exposures as (
    select
        cli.claim_id,
        cli.claim_line_number,
        cli.data_source,
        cli.person_id,
        cli.ingredient_rxcui,
        cli.ingredient_name,
        clw.adjusted_start_date as drug_exposure_start_date,
        clw.adjusted_end_date as drug_exposure_end_date,
        clw.days_supply
    from claim_line_ingredients cli
    inner join claim_line_with_end_dates clw
        on cli.claim_id = clw.claim_id
        and cli.claim_line_number = clw.claim_line_number
        and cli.data_source = clw.data_source
        and cli.person_id = clw.person_id
)

select * from final_ingredient_exposures
