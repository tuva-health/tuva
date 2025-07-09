-- ============================================================================
-- EMERGENCY DEPARTMENT ENCOUNTER MERGING LOGIC
-- ============================================================================
-- This query merges emergency department claims into logical encounters based on
-- overlapping dates, adjacent transfers, and same-facility criteria. The logic
-- is similar to acute inpatient merging but specific to ED encounters.
--
-- MERGE SCENARIOS:
-- 1. Same end date + same facility (concurrent claims)
-- 2. Adjacent dates (1 day apart) + same facility + transfer discharge (code 30)
-- 3. Overlapping dates + same facility (>= overlap, not strict >)

-- Step 1: Aggregate claim dates at the claim + patient + source level
-- This handles cases where a single claim might have multiple rows with different dates
with claim_start_end as (
    select claim_id
        , patient_data_source_id
        , min(start_date) as start_date
        , max(end_date) as end_date
    from {{ ref('encounters__stg_medical_claim') }}
    group by claim_id
        , patient_data_source_id
),

-- Step 2: Filter to emergency department claims only (both institutional and professional)
-- This is our target population for ED encounter merging
base as (
    select distinct enc.claim_id
        , enc.patient_data_source_id
        , c.start_date
        , c.end_date
        , enc.facility_npi
        , enc.discharge_disposition_code
    from {{ ref('encounters__stg_medical_claim') }} enc
    inner join claim_start_end c
        on enc.claim_id = c.claim_id
        and enc.patient_data_source_id = c.patient_data_source_id
    where enc.service_category_2 = 'emergency department'
),

-- Step 3: Add sequence numbers to order claims chronologically
-- Later claims (higher end_date) get higher row numbers
claims_sequenced as (
    select patient_data_source_id
        , claim_id
        , start_date
        , end_date
        , discharge_disposition_code
        , facility_npi
        , row_number() over (
            partition by patient_data_source_id 
            order by end_date, start_date, claim_id
        ) as row_num
    from base
),

-- Step 4: Identify which claims should be merged based on business rules
-- Three merge scenarios for ED encounters:
-- 1. Same end date + same facility (concurrent claims)
-- 2. Adjacent dates (1 day apart) + same facility + transfer discharge (code 30)
-- 3. Overlapping dates + same facility (note: >= for ED vs > for inpatient)
merge_candidates as (
    select a.patient_data_source_id
        , a.claim_id as claim_id_a
        , b.claim_id as claim_id_b
        , a.row_num as row_num_a
        , b.row_num as row_num_b
        , case
            -- Scenario 1: Concurrent claims at same facility
            when a.end_date = b.end_date 
                and a.facility_npi = b.facility_npi then 1
            
            -- Scenario 2: Adjacent transfer (next day after transfer discharge)
            when {{ dbt.dateadd(datepart='day', interval=1, from_date_or_timestamp='a.end_date') }} = b.start_date
                and a.facility_npi = b.facility_npi
                and a.discharge_disposition_code = '30' then 1
            
            -- Scenario 3: Overlapping stays at same facility
            -- Note: Using >= instead of > (different from inpatient logic)
            when a.end_date <> b.end_date
                and a.end_date >= b.start_date
                and a.facility_npi = b.facility_npi then 1
            else 0
        end as should_merge
    from claims_sequenced a
    inner join claims_sequenced b
        on a.patient_data_source_id = b.patient_data_source_id
        and a.row_num < b.row_num  -- Only compare earlier claims to later ones
        and a.claim_id <> b.claim_id
),

-- Step 5: Get confirmed merges (only pairs that should merge)
confirmed_merges as (
    select patient_data_source_id
        , claim_id_a
        , claim_id_b
        , row_num_a
        , row_num_b
    from merge_candidates
    where should_merge = 1
),

-- Step 6: Identify "closing" claims (claims that don't merge with any later claims)
-- These will become the encounter_id for their group
closing_claims as (
    select c.patient_data_source_id
        , c.claim_id
        , c.start_date
        , c.end_date
        , c.discharge_disposition_code
        , c.facility_npi
        , c.row_num
        , case
            -- A claim "closes" an encounter if:
            -- 1. It doesn't merge with any later claims AND
            -- 2. It's not in the middle of a merge chain
            when not exists (
                select 1 from confirmed_merges m1
                where m1.claim_id_a = c.claim_id
            ) and not exists (
                select 1 from confirmed_merges m2
                where m2.row_num_a < c.row_num 
                and m2.row_num_b > c.row_num
                and m2.patient_data_source_id = c.patient_data_source_id
            ) then 1
            else 0
        end as is_closing_claim
    from claims_sequenced c
),

-- Step 7: For each claim, find the earliest closing claim that comes after it
-- This determines which encounter group the claim belongs to
encounter_assignments as (
    select c.patient_data_source_id
        , c.claim_id
        , c.start_date
        , c.end_date
        , c.discharge_disposition_code
        , c.facility_npi
        , c.row_num
        , c.is_closing_claim
        , -- Find the minimum closing row number that comes at or after this claim
        (select min(closer.row_num)
         from closing_claims closer
         where closer.patient_data_source_id = c.patient_data_source_id
           and closer.row_num >= c.row_num
           and closer.is_closing_claim = 1
        ) as encounter_closing_row
    from closing_claims c
),

-- Step 8: Assign encounter_id by linking to the closing claim
encounters_with_ids as (
    select ea.patient_data_source_id
        , ea.claim_id
        , ea.start_date
        , ea.end_date
        , ea.discharge_disposition_code
        , ea.facility_npi
        , ea.row_num
        , ea.is_closing_claim
        , ea.encounter_closing_row
        , -- The encounter_id is the claim_id of the closing claim
        cc.claim_id as encounter_id
    from encounter_assignments ea
    left join closing_claims cc
        on ea.patient_data_source_id = cc.patient_data_source_id
        and ea.encounter_closing_row = cc.row_num
)

-- Final output: Add encounter-level sequencing and numbering
select patient_data_source_id
    , claim_id
    , start_date
    , end_date
    , discharge_disposition_code
    , facility_npi
    , -- Number claims within each encounter (earliest first)
    row_number() over (
        partition by encounter_id 
        order by start_date, end_date, claim_id
    ) as encounter_claim_number
    , -- Number claims within each encounter (latest first)
    row_number() over (
        partition by encounter_id 
        order by start_date desc, end_date desc, claim_id desc
    ) as encounter_claim_number_desc
    , is_closing_claim as close_flag
    , encounter_closing_row as min_closing_row
    , -- Create sequential encounter IDs across all patients
    dense_rank() over (order by encounter_id) as encounter_id
from encounters_with_ids