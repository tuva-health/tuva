-- ============================================================================
-- ENCOUNTER MERGING LOGIC
-- ============================================================================
-- This query merges acute inpatient institutional claims into logical encounters
-- based on overlapping dates, adjacent transfers, and same-facility criteria.
-- The output groups related claims under a single encounter_id.

-- Step 1: Filter to acute inpatient institutional claims only
-- This is our target population for encounter merging
-- Step 2: Aggregate useful attributes
-- This handles cases where a single claim might have multiple rows with different values
with base_claims as (
    select data_source
        , claim_id
        , patient_sk
        , max(facility_npi) as facility_npi
        , max(discharge_disposition_code) as discharge_disposition_code
        , min(start_date) as start_date
        , max(end_date) as end_date
    from {{ ref('encounters__stg_medical_claim') }} enc
    where enc.service_category_2 = 'acute inpatient'
        and enc.claim_type = 'institutional'
    group by data_source
        , claim_id
        , patient_sk
),

-- Step 3: Add sequence numbers to order claims chronologically
-- Later claims (higher end_date) get higher row numbers
claims_sequenced as (
    select data_source
        , claim_id
        , patient_sk
        , facility_npi
        , discharge_disposition_code
        , start_date
        , end_date
        , row_number() over (
            partition by patient_sk
            order by end_date, start_date, claim_id
        ) as row_num
    from base_claims
),

-- Step 4: Identify which claims should be merged based on business rules
-- Four merge scenarios:
-- 1. Same end date + same facility (concurrent claims)
-- 2. Adjacent dates (1 day apart) + same facility + still a patient (code 30)
-- 3. Same start date + same facility + still a patient (code 30)
-- 4. Overlapping dates + same facility
merge_candidates as (
    select a.row_num as row_num_a
        , b.row_num as row_num_b
        , case
            -- Scenario 1: Concurrent claims (Catches duplicates/corrections)
            when a.end_date = b.end_date
                and a.facility_npi = b.facility_npi then 1

            -- Scenario 2: Consecutive Stay with Transfer (Catches month-end billing)
            when {{ dbt.dateadd(datepart='day', interval=1, from_date_or_timestamp='a.end_date') }} = b.start_date
                and a.facility_npi = b.facility_npi
                and a.discharge_disposition_code = '30' then 1

            -- Scenario 3: Same-Day Start / Superseded Claim
            when a.start_date = b.start_date
                and a.facility_npi = b.facility_npi
                and a.discharge_disposition_code = '30' then 1

            -- Scenario 4: General Overlapping Stay
            when a.end_date != b.end_date
                and a.end_date > b.start_date
                and a.facility_npi = b.facility_npi then 1
            else 0
        end as should_merge
    from claims_sequenced a
        inner join claims_sequenced b
        on a.patient_sk = b.patient_sk
        and a.row_num < b.row_num  -- Only compare earlier claims to later ones
        and a.claim_id != b.claim_id
),

-- Step 5: Get confirmed merges (only pairs that should merge)
confirmed_merges as (
    select patient_sk
        , row_num_a
        , row_num_b
    from merge_candidates
    where should_merge = 1
),

-- Step 6: Identify "closing" claims (claims that don't merge with any later claims)
-- These will become the encounter_id for their group
closing_claims as (
    select c.data_source
        , c.claim_id
        , c.patient_sk
        , c.facility_npi
        , c.discharge_disposition_code
        , c.start_date
        , c.end_date
        , c.row_num
        , case
            -- A claim "closes" an encounter if:
            -- 1. It doesn't merge with any later claims AND
            -- 2. It's not in the middle of a merge chain
            when not exists (
                select 1 from confirmed_merges m1
                where m1.row_num_a = c.row_num
            ) and not exists (
                select 1 from confirmed_merges m2
                where m2.row_num_a < c.row_num
                    and m2.row_num_b > c.row_num
                    and m2.patient_sk = c.patient_sk
            ) then 1
            else 0
        end as is_closing_claim
    from claims_sequenced c
),

-- Step 7: For each claim, find the earliest closing claim that comes after it
-- This determines which encounter group the claim belongs to
encounter_assignments as (
    select c.data_source
        , c.claim_id
        , c.patient_sk
        , c.facility_npi
        , c.discharge_disposition_code
        , c.start_date
        , c.end_date
        , c.row_num
        , c.is_closing_claim
        , -- Find the minimum closing row number that comes at or after this claim
        (select min(closer.row_num)
         from closing_claims closer
         where closer.patient_sk = c.patient_sk
            and closer.row_num >= c.row_num
            and closer.is_closing_claim = 1
        ) as encounter_closing_row
    from closing_claims c
),

-- Step 8: Assign encounter_id by linking to the closing claim
encounters_with_ids as (
    select ea.data_source
        , ea.claim_id
        , ea.patient_sk
        , ea.facility_npi
        , ea.discharge_disposition_code
        , ea.start_date
        , ea.end_date
        , -- The encounter_group_id is the row_num of the closing claim
        ea.encounter_closing_row as encounter_id
    from encounter_assignments ea
    left join closing_claims cc
        on ea.patient_sk = cc.patient_sk
        and ea.encounter_closing_row = cc.row_num
)

-- Final output: Claims mapped to a derived encounter
select data_source
    , claim_id
    , patient_sk
    , encounter_id
    , facility_npi
    , discharge_disposition_code
    , min(start_date) over (partition by encounter_id) as encounter_start_date
    , max(end_date) over (partition by encounter_id) as encounter_end_date
from encounters_with_ids