-- ============================================================================
-- OVERLAPPING CLAIM ATTRIBUTION TO ENCOUNTERS
-- ============================================================================
-- This query attributes overlapping claims to multi-day encounters
-- based on date overlap. Each claim gets assigned to
-- the encounter whose date range contains the claim's start date.

with encounters__stg_prof_and_ancillary as (
    select *
    from {{ ref('encounters__stg_prof_and_ancillary') }}
),
encounters__stg_outpatient_institutional as (
    select *
    from {{ ref('encounters__stg_outpatient_institutional') }}
),
encounters as (
-- Step 1: Get encounter date ranges with patient information
-- We only need one row per encounter, so get encounter grain
    select distinct
        gei.encounter_id
        , gei.patient_sk
        , gei.encounter_type
        , gei.encounter_start_date
        , gei.encounter_end_date
    from {{ ref('encounters__int_inpatient__multi_claim_map') }} gei
)

-- Step 2: Attribution logic - match overlapping claims to encounters
-- Overlapping claims are attributed to encounters when:
-- 1. Same patient (patient_sk)
-- 2. Claim start date falls within encounter date range (inclusive)
-- 3. Claim is professional or outpatient institutional or service is ancillary.
select plp.medical_claim_sk
    , plp.data_source
    , plp.claim_id
    , enc.encounter_type
    , enc.encounter_id
    , enc.encounter_start_date
    , enc.encounter_end_date
    , -- Handle edge case: if a professional claim overlaps multiple encounters,
      -- prioritize institutional over professional, earliest claim, encounter_id (lowest wins) to ensure deterministic attribution
    row_number() over (
        partition by plp.medical_claim_sk
        order by enc.encounter_id
    ) as claim_priority
from encounters__stg_prof_and_ancillary as plp
    inner join encounters as enc
        on plp.patient_sk = enc.patient_sk
        and plp.start_date between enc.encounter_start_date and enc.encounter_end_date

union

select opi.medical_claim_sk
    , opi.data_source
    , opi.claim_id
    , enc.encounter_type
    , enc.encounter_id
    , enc.encounter_start_date
    , enc.encounter_end_date
    , -- Handle edge case: if a professional claim overlaps multiple encounters,
      -- prioritize by encounter_id (lowest wins) to ensure deterministic attribution
      -- TODO: Probably should prioritize by encounter_type priority? Ordering by encounter_id seems arbitrary.
    row_number() over (
        partition by opi.medical_claim_sk
        order by enc.encounter_id
    ) as claim_priority
from encounters__stg_outpatient_institutional as opi
    inner join encounters as enc
        on opi.patient_sk = enc.patient_sk
        and opi.start_date between enc.encounter_start_date and enc.encounter_end_date
-- in the original logic, this only gets merged for ED encounters.
-- where enc.encounter_type = 'emergency department'