-- ============================================================================
-- ENCOUNTER CLAIMS CROSSWALK UNION
-- ============================================================================
-- This model creates a comprehensive crosswalk between claims and encounters across
-- all encounter types. When claims are assigned to multiple encounters, priority
-- numbers determine the final assignment (lower number = higher priority).
--
-- PRIORITY HIERARCHY:
-- 0: Acute Inpatient (highest priority)
-- 1: Emergency Department, Inpatient Hospice  
-- 2: Inpatient Psych
-- 3: Inpatient Rehabilitation
-- 4: Inpatient Long Term Acute Care
-- 5: Inpatient Skilled Nursing
-- 6: Inpatient Substance Use
-- 7-8: Office Based (radiology has higher priority)
-- 9-20: Various Outpatient Types
-- 999: Outpatient Hospital/Clinic (catch-all)
-- 1M+: Orphaned encounters (lab, DME, ambulance)

with encounter_claims_union as (
    
    -- ========================================================================
    -- INPATIENT AND EMERGENCY ENCOUNTERS (Priority 0-6)
    -- ========================================================================
    
    -- Acute Inpatient - Professional Claims (Priority 0 - Highest)
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , 'acute inpatient' as encounter_type
        , 'inpatient' as encounter_group
        , 0 as priority_number
    from {{ ref('encounters__int_acute_inpatient__prof_claims') }}
    where claim_attribution_number = 1
    
    union all
    
    -- Acute Inpatient - Institutional Claims (Priority 0)
    select med.medical_claim_sk
        , enc.encounter_id
        , encounter_start_date
        , encounter_end_date
        , 'acute inpatient' as encounter_type
        , 'inpatient' as encounter_group
        , 0 as priority_number
    from {{ ref('encounters__int_acute_inpatient__generate_encounter_id') }} as enc
    inner join {{ ref('encounters__stg_medical_claim') }} med
        on enc.claim_id = med.claim_id
        and enc.data_source = med.data_source
    
    union all
    
    -- Emergency Department - Professional Claims (Priority 1)
    -- Note: Intentionally including professional claims assigned to inpatient 
    -- stays in case admit is assigned to ED
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , 'emergency department' as encounter_type
        , 'outpatient' as encounter_group
        , 1 as priority_number
    from {{ ref('encounters__int_acute_inpatient__prof_claims') }}
    where claim_attribution_number = 1
    
    union all
    
    -- Emergency Department - Institutional Claims (Priority 1)
    select med.medical_claim_sk
        , enc.encounter_id
        , enc.encounter_start_date
        , enc.encounter_end_date
        , 'emergency department' as encounter_type
        , 'outpatient' as encounter_group
        , 1 as priority_number
    from {{ ref('encounters__int_emergency_department__generate_encounter_id') }} enc
    inner join {{ ref('encounters__stg_medical_claim') }} med 
        on enc.claim_id = med.claim_id
    
    union all
    
    -- Emergency Department - Professional Claims (Priority 1)
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , 'emergency department' as encounter_type
        , 'outpatient' as encounter_group
        , 1 as priority_number
    from {{ ref('encounters__int_emergency_department__prof_claims') }}
    where claim_attribution_number = 1
    
    union all

    -- Office-Based Radiology (Priority 7)
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , encounter_type
        , 'office based' as encounter_group
        , 7 as priority_number
    from {{ ref('encounters__int_office_visits__generate_encounter_id') }}
    where encounter_type = 'office visit radiology'

    union all

    -- Office-Based non-radiology (Priority 8)
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , encounter_type
        , 'office based' as encounter_group
        , 8 as priority_number
    from {{ ref('encounters__int_office_visits__generate_encounter_id') }}
    where encounter_type <> 'office visit radiology'

    union all

    -- Ambulatory Surgery Cetner (Priority 12)
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , 'ambulatory surgery center' as encounter_type
        , 'outpatient' as encounter_group
        , 12 as priority_number
    from {{ ref('encounters__int_asc__generate_encounter_id') }}

    union all

    -- Dialysis (Priority 13)
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , 'dialysis' as encounter_type
        , 'outpatient' as encounter_group
        , 13 as priority_number
    from {{ ref('encounters__int_dialysis__generate_encounter_id') }}

    union all

    -- Home Health (Priority 15)
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , 'home health' as encounter_type
        , 'outpatient' as encounter_group
        , 15 as priority_number
    from {{ ref('encounters__int_home_health__generate_encounter_id') }}

    union all

    -- Durable Medical Equipment - Orphaned
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , 'dme - orphaned' as encounter_type
        , 'other' as encounter_group
        , 1000001 as priority_number
    from {{ ref('encounters__int_dme__generate_encounter_id') }}

    union all

    -- Ambulance - Orphaned
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , 'ambulance - orphaned' as encounter_type
        , 'other' as encounter_group
        , 1000002 as priority_number
    from {{ ref('encounters__int_ambulance__generate_encounter_id') }}
)

-- ============================================================================
-- FINAL OUTPUT: DEDUPLICATION AND ENCOUNTER ID ASSIGNMENT
-- ============================================================================
-- For claims assigned to multiple encounters, select the highest priority
-- (lowest priority_number) and assign sequential encounter IDs

select medical_claim_sk
    , encounter_id
    , encounter_start_date
    , encounter_end_date
    , -- Create new sequential encounter IDs grouped by type and original ID TODO: Change to a hash
    dense_rank() over (order by encounter_type, encounter_id) as encounter_sk
    , encounter_type
    , encounter_group
    , priority_number
    , -- Assign attribution number based on priority (1 = highest priority assignment)
    row_number() over (partition by medical_claim_sk order by priority_number) as encounter_type_priority
from encounter_claims_union