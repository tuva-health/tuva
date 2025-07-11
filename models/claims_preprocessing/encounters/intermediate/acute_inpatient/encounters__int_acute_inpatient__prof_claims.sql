-- ============================================================================
-- PROFESSIONAL CLAIM ATTRIBUTION TO ENCOUNTERS
-- ============================================================================
-- This query attributes professional (and other lower priority) claims to 
-- acute inpatient encounters based on date overlap. Each professional claim
-- gets assigned to the encounter whose date range contains the claim's start date.

-- Step 1: Get encounter date ranges with patient information
-- We only need one row per encounter, so get encounter grain
with encounters__prof_and_lower_priority as (
    select *
    from {{ ref('encounters__prof_and_lower_priority') }}
),
encounters as (
    select distinct
        gei.encounter_id
        , gei.patient_sk
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
    , plp.data_source
    , plp.claim_id
    , plp.claim_line_number
    , enc.encounter_id
    , -- Handle edge case: if a professional claim overlaps multiple encounters,
      -- prioritize by encounter_id (lowest wins) to ensure deterministic attribution
    row_number() over (
        partition by plp.medical_claim_sk
        order by enc.encounter_id
    ) as claim_attribution_number
from encounters__prof_and_lower_priority as plp
    -- Match claims to encounters based on patient and date overlap
    inner join encounters as enc
        on plp.patient_sk = enc.patient_sk
        and plp.start_date between enc.encounter_start_date and enc.encounter_end_date