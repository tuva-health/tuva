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
-- 1000+: Orphaned encounters (lab, DME, ambulance)

with encounters__stg_medical_claim as (
    select *
    from {{ ref('encounters__stg_medical_claim') }}
),
encounter_claims_union as (
    
    -- ========================================================================
    -- INPATIENT AND EMERGENCY ENCOUNTERS (Priority 0-6)
    -- ========================================================================
    
    -- Acute Inpatient - Professional Claims
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , 'acute inpatient' as encounter_type
        , 'inpatient' as encounter_group
        , 0 as priority_number
    from {{ ref('encounters__int_multi_day__single_claim_map') }}
    where claim_priority = 1
    
    union all
    
    -- Acute Inpatient - Institutional Claims
    select med.medical_claim_sk
        , enc.encounter_id
        , enc.encounter_start_date
        , enc.encounter_end_date
        , 'acute inpatient' as encounter_type
        , 'inpatient' as encounter_group
        , 0 as priority_number
    from {{ ref('encounters__int_acute_inpatient__multi_claim_map') }} as enc
        inner join encounters__stg_medical_claim as med
        on enc.data_source = med.data_source
        and enc.claim_id = med.claim_id

    union all
    
    -- Emergency Department - Professional Claims
    -- Note: Intentionally including professional claims assigned to inpatient 
    -- stays in case admit is assigned to ED
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , 'emergency department' as encounter_type
        , 'outpatient' as encounter_group
        , 1 as priority_number
    from {{ ref('encounters__int_acute_inpatient__single_claim_map') }}
    where claim_priority = 1

    union all

    -- Emergency Department - Professional Claims
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , 'emergency department' as encounter_type
        , 'outpatient' as encounter_group
        , 1 as priority_number
    from {{ ref('encounters__int_emergency_department__single_claim_map') }}
    where claim_priority = 1

    union all
    
    -- Emergency Department - Institutional Claims
    select med.medical_claim_sk
        , enc.encounter_id
        , enc.encounter_start_date
        , enc.encounter_end_date
        , 'emergency department' as encounter_type
        , 'outpatient' as encounter_group
        , 1 as priority_number
    from {{ ref('encounters__int_emergency_department__multi_claim_map') }} enc
        inner join encounters__stg_medical_claim as med
        on enc.data_source = med.data_source
        and enc.claim_id = med.claim_id

    union all

    -- Inpatient Psych - Professional Claims
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , 'inpatient psych' as encounter_type
        , 'inpatient' as encounter_group
        , 2 as priority_number
    from {{ ref('encounters__int_multi_day__single_claim_map') }}
    where claim_priority = 1
        and encounter_type = 'inpatient psych'

    union all

    -- Inpatient Psych - Institutional Claims
    select med.medical_claim_sk
        , enc.encounter_id
        , enc.encounter_start_date
        , enc.encounter_end_date
        , 'inpatient psych' as encounter_type
        , 'inpatient' as encounter_group
        , 2 as priority_number
    from {{ ref('encounters__int_multi_day__multi_claim_map') }} as enc
        inner join encounters__stg_medical_claim as med
        on enc.data_source = med.data_source
        and enc.claim_id = med.claim_id
    where encounter_type = 'inpatient psych'

    union all

    -- Inpatient Rehabilitation
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , 'inpatient rehabilitation' as encounter_type
        , 'inpatient' as encounter_group
        , 3 as priority_number
    from {{ ref('encounters__int_multi_day__single_claim_map') }}
    where claim_priority = 1
        and encounter_type = 'inpatient rehabilitation'

    union all

    -- Inpatient Rehabilitation
    select med.medical_claim_sk
        , enc.encounter_id
        , enc.encounter_start_date
        , enc.encounter_end_date
        , 'inpatient rehabilitation' as encounter_type
        , 'inpatient' as encounter_group
        , 3 as priority_number
    from {{ ref('encounters__int_multi_day__multi_claim_map') }} as enc
        inner join encounters__stg_medical_claim as med
        on enc.data_source = med.data_source
        and enc.claim_id = med.claim_id
    where encounter_type = 'inpatient rehabilitation'

    union all

    -- Inpatient Long Term Acute Care
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , 'inpatient long term acute care' as encounter_type
        , 'inpatient' as encounter_group
        , 4 as priority_number
    from {{ ref('encounters__int_multi_day__single_claim_map') }}
    where claim_priority = 1
        and encounter_type = 'inpatient long term acute care'

    union all

    -- Inpatient Long Term Acute Care
    select med.medical_claim_sk
        , enc.encounter_id
        , enc.encounter_start_date
        , enc.encounter_end_date
        , 'inpatient long term acute care' as encounter_type
        , 'inpatient' as encounter_group
        , 4 as priority_number
    from {{ ref('encounters__int_multi_day__multi_claim_map') }} as enc
        inner join encounters__stg_medical_claim as med
        on enc.data_source = med.data_source
        and enc.claim_id = med.claim_id
    where encounter_type = 'inpatient long term acute care'


    union all

    -- Inpatient SNF
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , 'inpatient skilled nursing' as encounter_type
        , 'inpatient' as encounter_group
        , 5 as priority_number
    from {{ ref('encounters__int_multi_day__single_claim_map') }}
    where claim_priority = 1
        and encounter_type = 'inpatient skilled nursing'

    union all

    -- Inpatient SNF
    select med.medical_claim_sk
        , enc.encounter_id
        , enc.encounter_start_date
        , enc.encounter_end_date
        , 'inpatient skilled nursing' as encounter_type
        , 'inpatient' as encounter_group
        , 5 as priority_number
    from {{ ref('encounters__int_multi_day__multi_claim_map') }} as enc
        inner join encounters__stg_medical_claim as med
        on enc.data_source = med.data_source
        and enc.claim_id = med.claim_id
    where encounter_type = 'skilled nursing'

    union all

    -- Inpatient Substance Use
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , 'inpatient substance use' as encounter_type
        , 'inpatient' as encounter_group
        , 6 as priority_number
    from {{ ref('encounters__int_multi_day__single_claim_map') }}
    where claim_priority = 1
        and encounter_type = 'inpatient substance use'

    union all

    -- Inpatient Substance Use
    select med.medical_claim_sk
        , enc.encounter_id
        , enc.encounter_start_date
        , enc.encounter_end_date
        , 'inpatient substance use' as encounter_type
        , 'inpatient' as encounter_group
        , 6 as priority_number
    from {{ ref('encounters__int_multi_day__multi_claim_map') }} as enc
        inner join encounters__stg_medical_claim as med
        on enc.data_source = med.data_source
        and enc.claim_id = med.claim_id
    where encounter_type = 'inpatient substance use'

    -- ========================================================================
    -- OFFICE BASED
    -- ========================================================================

    union all

    -- Office-Based Radiology
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , encounter_type
        , 'office based' as encounter_group
        , 7 as priority_number
    from {{ ref('encounters__int_office_visits__single_claim_map') }}
    where encounter_type = 'office visit radiology'

    union all

    -- Office-Based non-radiology
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , encounter_type
        , 'office based' as encounter_group
        , 8 as priority_number
    from {{ ref('encounters__int_office_visits__single_claim_map') }}
    where encounter_type <> 'office visit radiology'

    -- ========================================================================
    -- VARIOUS OUTPATIENT TYPES
    -- ========================================================================

    union all

    -- Urgent care is set at lower priority than ED and inpatient to avoid over flagging urgent
    -- care due to variations in billing practices.
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , 'urgent care' as encounter_type
        , 'outpatient' as encounter_group
        , 9 as priority_number
    from {{ ref('encounters__int_single_day__single_claim_map') }}
    where service_category_2 = 'urgent care'

    union all

    -- Outpatient Psych
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , 'outpatient psych' as encounter_type
        , 'outpatient' as encounter_group
        , 10 as priority_number
    from {{ ref('encounters__int_single_day__single_claim_map') }}
    where service_category_2 = 'outpatient psych'

    union all

    -- Outpatient Rehabilitation
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , 'outpatient rehabilitation' as encounter_type
        , 'outpatient' as encounter_group
        , 11 as priority_number
    from {{ ref('encounters__int_single_day__single_claim_map') }}
    where service_category_2 = 'outpatient rehabilitation'

    union all

    -- Ambulatory Surgery Center
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , 'ambulatory surgery center' as encounter_type
        , 'outpatient' as encounter_group
        , 12 as priority_number
    from {{ ref('encounters__int_single_day__single_claim_map') }}
    where service_category_2 = 'ambulatory surgery center'

    union all

    -- Dialysis
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , 'dialysis' as encounter_type
        , 'outpatient' as encounter_group
        , 13 as priority_number
    from {{ ref('encounters__int_single_day__single_claim_map') }}
    where  service_category_2 = 'dialysis'

    union all

    -- Outpatient Hospice
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , 'outpatient hospice' as encounter_type
        , 'outpatient' as encounter_group
        , 14 as priority_number
    from {{ ref('encounters__int_single_day__single_claim_map') }}
    where  service_category_2 = 'outpatient hospice'

    union all

    -- Home Health
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , 'home health' as encounter_type
        , 'outpatient' as encounter_group
        , 15 as priority_number
    from {{ ref('encounters__int_single_day__single_claim_map') }}
    where  service_category_2 = 'home health'

    union all

    -- Outpatient Surgery
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , 'outpatient surgery' as encounter_type
        , 'outpatient' as encounter_group
        , 16 as priority_number
    from {{ ref('encounters__int_single_day__single_claim_map') }}
    where service_category_2 = 'outpatient surgery'


    union all

    -- Outpatient Injections
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , 'outpatient injections' as encounter_type
        , 'outpatient' as encounter_group
        , 17 as priority_number
    from {{ ref('encounters__int_single_day__single_claim_map') }}
    where injection_flag = 1

    union all

    -- Outpatient PT/OT/ST
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , 'outpatient pt/ot/st' as encounter_type
        , 'outpatient' as encounter_group
        , 18 as priority_number
    from {{ ref('encounters__int_single_day__single_claim_map') }}
    where service_category_2 = 'outpatient pt/ot/st'

    union all

    -- Outpatient Substance Use
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , 'outpatient substance use' as encounter_type
        , 'outpatient' as encounter_group
        , 19 as priority_number
    from {{ ref('encounters__int_single_day__single_claim_map') }}
    where service_category_2 = 'outpatient substance use'

    union all

    -- Outpatient Radiology
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , 'outpatient radiology' as encounter_type
        , 'outpatient' as encounter_group
        , 20 as priority_number
    from {{ ref('encounters__int_single_day__single_claim_map') }}
    where service_category_2 = 'outpatient radiology'

    union all

    -- Observation and Other Outpatient
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , 'outpatient hospital or clinic' as encounter_type
        , 'outpatient' as encounter_group
        , 999 as priority_number
    from {{ ref('encounters__int_single_day__single_claim_map') }}
    where service_category_2 in ('observation', 'outpatient hospital or clinic')

    -- ========================================================================
    -- ORPHANED ENCOUNTERS
    -- ========================================================================

    union all

    -- Lab - Orphaned
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , 'lab - orphaned' as encounter_type
        , 'other' as encounter_group
        , 1000 as priority_number
    from {{ ref('encounters__int_single_day__single_claim_map') }}
    where  service_category_2 = 'lab'

    union all

    -- Durable Medical Equipment - Orphaned
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , 'dme - orphaned' as encounter_type
        , 'other' as encounter_group
        , 1001 as priority_number
    from {{ ref('encounters__int_single_day__single_claim_map') }}
    where  service_category_2 = 'dme'

    union all

    -- Ambulance - Orphaned
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , 'ambulance - orphaned' as encounter_type
        , 'other' as encounter_group
        , 1002 as priority_number
    from {{ ref('encounters__int_single_day__single_claim_map') }}
    where  service_category_2 = 'ambulance'
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