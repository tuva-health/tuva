{{ config(
    enabled = var('brand_generic_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
) }}

with recursive drug_exposure_input as (
   select * from {{ ref('pharmacy__stg_add_ingredient_concepts') }}
),

-- Add row numbers for processing order
numbered_exposures as (
   select
       person_id,
       ingredient_rxcui,
       ingredient_name,
       drug_exposure_start_date,
       drug_exposure_end_date,
       days_supply,
       row_number() over(
           partition by person_id, ingredient_rxcui
           order by drug_exposure_start_date, drug_exposure_end_date
       ) as rn
   from drug_exposure_input
),

-- Recursive CTE to compute sequential adjustments
-- Each row's adjusted dates depend on the previous row's adjusted dates
recursive_adjustments as (
   -- Base case: first fill for each person + ingredient combination
   select
       person_id,
       ingredient_rxcui,
       ingredient_name,
       drug_exposure_start_date,
       drug_exposure_end_date,
       days_supply,
       rn,
       -- First fill: no adjustment needed
       drug_exposure_start_date as adjusted_start_date,
       case
           when days_supply > 0 then drug_exposure_start_date + interval '1 day' * (days_supply - 1)
           else drug_exposure_start_date
       end as adjusted_end_date
   from numbered_exposures
   where rn = 1
   
   union all
   
   -- Recursive case: subsequent fills
   select
       ne.person_id,
       ne.ingredient_rxcui,
       ne.ingredient_name,
       ne.drug_exposure_start_date,
       ne.drug_exposure_end_date,
       ne.days_supply,
       ne.rn,
       -- Adjusted start: later of (original start, prior adjusted end + 1 day)
       greatest(
           ne.drug_exposure_start_date,
           ra.adjusted_end_date + interval '1 day'
       ) as adjusted_start_date,
       -- Adjusted end: adjusted start + days_supply - 1
       greatest(
           ne.drug_exposure_start_date,
           ra.adjusted_end_date + interval '1 day'
       ) + case
           when ne.days_supply > 0 then interval '1 day' * (ne.days_supply - 1)
           else interval '0 day'
       end as adjusted_end_date
   from numbered_exposures ne
   inner join recursive_adjustments ra
       on ne.person_id = ra.person_id
       and ne.ingredient_rxcui = ra.ingredient_rxcui
       and ne.rn = ra.rn + 1  -- Sequential processing
),

-- Detect gaps and assign group IDs based on adjusted dates
exposures_with_gaps as (
   select
       person_id,
       ingredient_rxcui,
       ingredient_name,
       adjusted_start_date as drug_exposure_start_date,
       adjusted_end_date as drug_exposure_end_date,
       -- Get previous adjusted end date for gap detection
       lag(adjusted_end_date) over(
           partition by person_id, ingredient_rxcui 
           order by adjusted_start_date
       ) as prev_adjusted_end_date
   from recursive_adjustments
),

-- Assign group IDs based on gaps > 1 day after adjustment
exposures_with_groups as (
   select
       person_id,
       ingredient_rxcui,
       ingredient_name,
       drug_exposure_start_date,
       drug_exposure_end_date,
       -- Create group IDs: increment when there's a gap > 1 day
       sum(case
           when drug_exposure_start_date > coalesce(
               prev_adjusted_end_date + interval '1 day', 
               drug_exposure_start_date - interval '1 day'
           ) 
           then 1 
           else 0 
       end) over(
           partition by person_id, ingredient_rxcui
           order by drug_exposure_start_date
           rows between unbounded preceding and current row
       ) as group_id
   from exposures_with_gaps
)

-- Final: Aggregate by group to create sub-exposures
select
   person_id,
   ingredient_rxcui,
   ingredient_name,
   min(drug_exposure_start_date) as drug_sub_exposure_start_date,
   max(drug_exposure_end_date) as drug_sub_exposure_end_date,
   count(*) as drug_exposure_count,
   -- days_exposed correctly represents sequential coverage
   datediff('day', min(drug_exposure_start_date), max(drug_exposure_end_date)) + 1 as days_exposed
from exposures_with_groups
group by
   person_id,
   ingredient_rxcui,
   ingredient_name,
   group_id
