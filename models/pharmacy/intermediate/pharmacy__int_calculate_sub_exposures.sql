{{ config(
    enabled = var('brand_generic_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
) }}

with recursive pharmacy_claim_input as (
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

-- Step 3: Create unique claims (collapse claim lines to claim level)
-- This assumes all lines in a claim have the same dates
unique_claims as (
    select distinct
        claim_id,
        data_source,
        person_id,
        min(original_start_date) as original_start_date,
        max(original_end_date) as original_end_date,
        max(days_supply) as days_supply
    from claim_line_ingredients
    group by
        claim_id,
        data_source,
        person_id
),

-- Step 4: Get all ingredients for each claim
claim_ingredients as (
    select distinct
        claim_id,
        data_source,
        person_id,
        ingredient_rxcui,
        ingredient_name
    from claim_line_ingredients
),

-- Step 5: Build dependency graph - find the most recent prior claim that shares any ingredient
-- For each claim, find its "parent" claim (the most recent prior claim sharing an ingredient)
claim_dependencies as (
    select
        c.claim_id,
        c.data_source,
        c.person_id,
        c.original_start_date,
        c.original_end_date,
        c.days_supply,
        -- Find the most recent prior claim that shares any ingredient
        max(prior.claim_id) as parent_claim_id
    from unique_claims c
    left join unique_claims prior
        on c.person_id = prior.person_id
        and c.data_source = prior.data_source
        and prior.original_start_date < c.original_start_date  -- Prior claim
        -- Check if they share any ingredient
        and exists (
            select 1
            from claim_ingredients ci1
            inner join claim_ingredients ci2
                on ci1.ingredient_rxcui = ci2.ingredient_rxcui
                and ci1.person_id = ci2.person_id
                and ci1.data_source = ci2.data_source
            where ci1.claim_id = c.claim_id
              and ci1.data_source = c.data_source
              and ci2.claim_id = prior.claim_id
              and ci2.data_source = prior.data_source
        )
    group by
        c.claim_id,
        c.data_source,
        c.person_id,
        c.original_start_date,
        c.original_end_date,
        c.days_supply
),

-- Step 6: RECURSIVE CTE to adjust claims based on dependencies
adjusted_claims as (
    -- Base case: Claims with no parent (first claims for each person or claims with no shared ingredients)
    select
        claim_id,
        data_source,
        person_id,
        original_start_date,
        original_end_date,
        days_supply,
        parent_claim_id,
        original_start_date as adjusted_start_date,
        original_end_date as adjusted_end_date,
        0 as recursion_depth
    from claim_dependencies
    where parent_claim_id is null
    
    union all
    
    -- Recursive case: Claims with a parent
    select
        cd.claim_id,
        cd.data_source,
        cd.person_id,
        cd.original_start_date,
        cd.original_end_date,
        cd.days_supply,
        cd.parent_claim_id,
        -- Adjusted start: later of (original start, parent's adjusted end + 1 day)
        greatest(
            cd.original_start_date,
            {{ dbt.dateadd('day', 1, 'ac.adjusted_end_date') }}
        ) as adjusted_start_date,
        -- Adjusted end: adjusted start + days_supply - 1
        {{ dbt.dateadd(
            'day',
            'cd.days_supply - 1',
            'greatest(
                cd.original_start_date,
                ' ~ dbt.dateadd('day', 1, 'ac.adjusted_end_date') ~ '
            )'
        ) }} as adjusted_end_date,
        ac.recursion_depth + 1 as recursion_depth
    from claim_dependencies cd
    inner join adjusted_claims ac
        on cd.parent_claim_id = ac.claim_id
        and cd.data_source = ac.data_source
        and cd.person_id = ac.person_id
    where ac.recursion_depth < 100  -- Prevent infinite recursion
),

-- Step 7: Join adjusted dates back to all claim lines and ingredients
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
    inner join adjusted_claims ac
        on cli.claim_id = ac.claim_id
        and cli.data_source = ac.data_source
        and cli.person_id = ac.person_id
)

select * from final_ingredient_exposures
