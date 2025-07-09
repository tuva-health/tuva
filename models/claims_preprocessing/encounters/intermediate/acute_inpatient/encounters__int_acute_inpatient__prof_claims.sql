-- ============================================================================
-- PROFESSIONAL CLAIM ATTRIBUTION TO ENCOUNTERS
-- ============================================================================
-- This query attributes professional (and other lower priority) claims to 
-- acute inpatient encounters based on date overlap. Each professional claim
-- gets assigned to the encounter whose date range contains the claim's start date.

-- Step 1: Get encounter date ranges with patient information
-- We only need one row per encounter, so filter to the first claim and join dates
with encounters_with_dates as (
    select distinct
        gei.encounter_id
        , gei.member_id
        , gei.data_source
        , gei.encounter_start_date
        , gei.encounter_end_date
    from {{ ref('encounters__int_acute_inpatient__generate_encounter_id') }} gei
)

-- Step 2: Attribution logic - match professional claims to encounters
-- Professional claims are attributed to encounters when:
-- 1. Same patient (patient_data_source_id)
-- 2. Claim start date falls within encounter date range (inclusive)
-- 3. Claim is professional or lower priority type
select plp.medical_claim_sk
    , plp.claim_id
    , plp.claim_line_number
    , plp.data_source
    , ewd.encounter_id
    , -- Handle edge case: if a professional claim overlaps multiple encounters,
      -- prioritize by encounter_id (lowest wins) to ensure deterministic attribution
    row_number() over (
        partition by plp.medical_claim_sk
        order by ewd.encounter_id
    ) as claim_attribution_number
from {{ ref('encounters__prof_and_lower_priority') }} as plp
    -- Match claims to encounters based on patient and date overlap
    inner join encounters_with_dates ewd
        on plp.data_source = ewd.data_source
        and plp.member_id = ewd.member_id
        and plp.start_date between ewd.encounter_start_date and ewd.encounter_end_date