{{ config(
    enabled = var('brand_generic_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
) }}

with pharmacy_claim_input as (
    select * from {{ ref('pharmacy_claim') }}
),

product_to_ingredient as (
    select * from {{ ref('pharmacy__product_to_ingredient') }}
),

-- Step 1: Prepare claim lines with calculated end dates and therapy grouping key
prepared_claim_lines as (
    select
        claim_id,
        claim_line_number,
        data_source,
        person_id,
        ndc_code,
        -- Therapy grouping key: only refills of the same NDC should adjust each other
        -- Different medications (even filled same day) should run concurrently
        ndc_code as therapy_key,
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

-- Step 2: Number claim lines for sequential processing WITHIN each therapy
-- Partition by person_id AND therapy_key so only same-medication refills adjust each other
numbered_claim_lines as (
    select
        claim_id,
        claim_line_number,
        data_source,
        person_id,
        ndc_code,
        therapy_key,
        dispensing_date as original_start_date,
        original_end_date,
        days_supply,
        row_number() over(
            partition by person_id, therapy_key  -- Only sequence within same therapy
            order by dispensing_date, claim_id, claim_line_number, data_source
        ) as rn
    from prepared_claim_lines
),

-- Step 3: RECURSIVE CLAIM LINE ADJUSTMENT WITHIN THERAPY GROUPS
-- Recursion chains only within same person AND therapy_key
-- This ensures unrelated medications are not serialized
recursive_claim_line_adjustments as (
    -- Base case: first claim line for each person + therapy combination
    select
        claim_id,
        claim_line_number,
        data_source,
        person_id,
        ndc_code,
        therapy_key,
        original_start_date,
        original_end_date,
        days_supply,
        rn,
        original_start_date as adjusted_start_date,
        original_end_date as adjusted_end_date
    from numbered_claim_lines
    where rn = 1
    
    union all
    
    -- Recursive case: subsequent claim lines within same therapy
    select
        ncl.claim_id,
        ncl.claim_line_number,
        ncl.data_source,
        ncl.person_id,
        ncl.ndc_code,
        ncl.therapy_key,
        ncl.original_start_date,
        ncl.original_end_date,
        ncl.days_supply,
        ncl.rn,
        -- Adjusted start: later of (original start, prior adjusted end + 1 day)
        greatest(
            ncl.original_start_date,
            {{ dbt.dateadd('day', 1, 'rcla.adjusted_end_date') }}
        ) as adjusted_start_date,
        -- Adjusted end: adjusted start + days_supply - 1
        {{ dbt.dateadd(
            'day',
            'ncl.days_supply - 1',
            greatest(
                ncl.original_start_date,
                {{ dbt.dateadd('day', 1, 'rcla.adjusted_end_date') }}
            )
        ) }} as adjusted_end_date
    from numbered_claim_lines ncl
    inner join recursive_claim_line_adjustments rcla
        on ncl.person_id = rcla.person_id
        and ncl.therapy_key = rcla.therapy_key  -- Must match therapy group
        and ncl.rn = rcla.rn + 1  -- Sequential within therapy
),

-- Step 4: Explode to ingredients, inheriting adjusted dates from claim line
-- All ingredients from the same claim line inherit the same adjusted dates
add_ingredient as (
    select
        rcla.claim_id,
        rcla.claim_line_number,
        rcla.data_source,
        rcla.person_id,
        pti.ingredient_rxcui,
        pti.ingredient_name,
        rcla.adjusted_start_date as drug_exposure_start_date,
        rcla.adjusted_end_date as drug_exposure_end_date,
        rcla.days_supply
    from recursive_claim_line_adjustments rcla
    inner join product_to_ingredient pti
        on rcla.ndc_code = pti.ndc
)

select * from add_ingredient
