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
    from {{ ref('encounters__stg_prof_and_lower_priority') }}
),
encounters as (
    select distinct
        gei.encounter_id
        , gei.patient_sk
        , gei.encounter_type
        , gei.encounter_start_date
        , gei.encounter_end_date
    from {{ ref('encounters__int_multi_day__multi_claim_map') }} gei
)

-- Step 2: Attribution logic - match professional claims to encounters
-- Professional claims are attributed to encounters when:
-- 1. Same patient (patient_sk)
-- 2. Claim start date falls within encounter date range (inclusive)
-- 3. Claim is professional or lower priority type
select plp.medical_claim_sk
    , plp.data_source
    , plp.claim_id
    , enc.encounter_type
    , enc.encounter_id
    , enc.encounter_start_date
    , enc.encounter_end_date
    , -- Handle edge case: if a professional claim overlaps multiple encounters,
      -- prioritize by encounter_id (lowest wins) to ensure deterministic attribution
      -- TODO: Probably should prioritize by the service category priority?
    row_number() over (
        partition by plp.medical_claim_sk
        order by enc.encounter_id
    ) as claim_priority
from encounters__prof_and_lower_priority as plp
    inner join encounters as enc
        on plp.patient_sk = enc.patient_sk
        and plp.start_date between enc.encounter_start_date and enc.encounter_end_date