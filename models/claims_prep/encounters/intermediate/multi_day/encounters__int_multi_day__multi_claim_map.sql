-- ============================================================================
-- ENCOUNTER MERGING LOGIC
-- ============================================================================
-- This query merges claims into logical encounters based on
-- overlapping dates, adjacent transfers, and same-facility criteria.

-- Step 1: Filter to the specific services that fit this scenario.
-- Aggregate claim dates at the claim + patient + source level. This handles cases
-- where a single claim might have multiple rows with different dates
with base_claims as (
    select data_source
        , claim_id
        , patient_sk
        , service_category_2 as encounter_type
        , max(facility_npi) as facility_npi
        , max(discharge_disposition_code) as discharge_disposition_code
        , min(start_date) as start_date
        , max(end_date) as end_date
    from {{ ref('encounters__stg_medical_claim') }}
    where claim_type = 'institutional'
    and service_category_2 in (
        'acute inpatient'
        , 'ambulatory surgery center'
        , 'emergency department'
        , 'inpatient hospice'
        , 'inpatient long term acute care'
        , 'inpatient psych'
        , 'inpatient rehabilitation'
        , 'inpatient snf'
        , 'inpatient substance use'
        , 'skilled nursing'
        )
    group by data_source
        , claim_id
        , patient_sk
        , service_category_2
),

-- Step 2: Add sequence numbers to order claims chronologically
-- Later claims (higher end_date) get higher row numbers
claims_sequenced as (
    select data_source
        , claim_id
        , patient_sk
        , encounter_type
        , -- facility_npi can often be null. in this case, we'd still want rows to merge so we put in a dummy value.
        coalesce(facility_npi, '') as facility_npi
        , discharge_disposition_code
        , start_date
        , end_date
        , row_number() over (
            partition by patient_sk, encounter_type
            order by end_date, start_date, claim_id
        ) as row_num
    from base_claims
),

-- Step 3: Identify which claims should be merged based on business rules
-- Four merge scenarios:
-- 1. Same end date + same facility (concurrent claims)
-- 2. Adjacent dates (1 day apart) + same facility + still a patient (code 30)
-- 3. Same start date + same facility + still a patient (code 30)
-- 4. Overlapping dates + same facility
merge_candidates as (
    select a.patient_sk
        , a.encounter_type
        , a.row_num as row_num_a
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
        and a.encounter_type = b.encounter_type
        and a.row_num < b.row_num  -- Only compare earlier claims to later ones
        and a.claim_id != b.claim_id
),

-- Step 4: Get confirmed merges (only pairs that should merge)
confirmed_merges as (
    select patient_sk
        , encounter_type
        , row_num_a
        , row_num_b
    from merge_candidates
    where should_merge = 1
),

-- Step 5: Identify "closing" claims (claims that don't merge with any later claims)
-- These will become the encounter_id for their group
closing_claims as (
    select c.data_source
        , c.claim_id
        , c.patient_sk
        , c.encounter_type
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
                    and m1.patient_sk = c.patient_sk
                    and m1.encounter_type = c.encounter_type
            ) and not exists (
                select 1 from confirmed_merges m2
                where m2.row_num_a < c.row_num
                    and m2.row_num_b > c.row_num
                    and m2.patient_sk = c.patient_sk
                    and m2.encounter_type = c.encounter_type
            ) then 1
            else 0
        end as is_closing_claim
    from claims_sequenced c
),

-- Step 6: For each claim, find the earliest closing claim that comes after it
-- This determines which encounter group the claim belongs to
encounter_assignments as (
    select c.data_source
        , c.claim_id
        , c.patient_sk
        , c.encounter_type
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
            and closer.encounter_type = c.encounter_type
            and closer.row_num >= c.row_num
            and closer.is_closing_claim = 1
        ) as encounter_closing_row
    from closing_claims c
),

-- Step 7: Assign encounter_id by linking to the closing claim
encounters_with_ids as (
    select ea.data_source
        , ea.claim_id
        , ea.patient_sk
        , ea.encounter_type
        , ea.start_date
        , ea.end_date
        , -- Generated ID = closing claim identifier as this is unique in this case
        {{ dbt_utils.generate_surrogate_key(['cc.data_source', 'cc.claim_id']) }} as encounter_id
    from encounter_assignments ea
    left join closing_claims cc
        on ea.patient_sk = cc.patient_sk
        and ea.encounter_type = cc.encounter_type
        and ea.encounter_closing_row = cc.row_num
)

-- Final output: Claims mapped to a derived encounter
select data_source
    , claim_id
    , patient_sk
    , encounter_type
    , encounter_id
    , min(start_date) over (partition by encounter_id) as encounter_start_date
    , max(end_date) over (partition by encounter_id) as encounter_end_date
from encounters_with_ids