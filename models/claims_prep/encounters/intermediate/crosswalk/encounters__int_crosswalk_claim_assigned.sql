-- ============================================================================
-- ENCOUNTER CLAIMS CROSSWALK
-- ============================================================================
-- This model creates a comprehensive crosswalk between claims and encounters across
-- all encounter types. When claims are assigned to multiple encounters, priority
-- numbers determine the final assignment (lower number = higher priority).
--
-- PRIORITY HIERARCHY:
-- 0: Acute Inpatient (highest priority)
-- 1: Emergency Department
-- 2-9: Inpatient
-- 10-19: Office Based
-- 20-29: Outpatient
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
        and encounter_type = 'acute inpatient'
    
    union all
    
    -- Acute Inpatient - Institutional Claims
    select med.medical_claim_sk
        , enc.encounter_id
        , enc.encounter_start_date
        , enc.encounter_end_date
        , 'acute inpatient' as encounter_type
        , 'inpatient' as encounter_group
        , 0 as priority_number
    from {{ ref('encounters__int_multi_day__multi_claim_map') }} as enc
        inner join encounters__stg_medical_claim as med
        on enc.data_source = med.data_source
        and enc.claim_id = med.claim_id
    where encounter_type = 'acute inpatient'

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
    from {{ ref('encounters__int_multi_day__single_claim_map') }}
    where claim_priority = 1
        and encounter_type = 'acute inpatient'

    union all

    -- Emergency Department - Professional Claims
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , 'emergency department' as encounter_type
        , 'outpatient' as encounter_group
        , 1 as priority_number
    from {{ ref('encounters__int_multi_day__single_claim_map') }}
    where claim_priority = 1
        and encounter_type = 'emergency department'

    union all
    
    -- Emergency Department - Institutional Claims
    select med.medical_claim_sk
        , enc.encounter_id
        , enc.encounter_start_date
        , enc.encounter_end_date
        , 'emergency department' as encounter_type
        , 'outpatient' as encounter_group
        , 1 as priority_number
    from {{ ref('encounters__int_multi_day__multi_claim_map') }} enc
        inner join encounters__stg_medical_claim as med
        on enc.data_source = med.data_source
        and enc.claim_id = med.claim_id
    where encounter_type = 'emergency department'

    union all

    -- Inpatient Hospice - Professional Claims
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , 'inpatient hospice' as encounter_type
        , 'inpatient' as encounter_group
        , 2 as priority_number
    from {{ ref('encounters__int_multi_day__single_claim_map') }}
    where claim_priority = 1
        and encounter_type = 'inpatient hospice'

    union all

    -- Inpatient Hospice - Institutional Claims
    select med.medical_claim_sk
        , enc.encounter_id
        , enc.encounter_start_date
        , enc.encounter_end_date
        , 'inpatient hospice' as encounter_type
        , 'inpatient' as encounter_group
        , 2 as priority_number
    from {{ ref('encounters__int_multi_day__multi_claim_map') }} as enc
        inner join encounters__stg_medical_claim as med
        on enc.data_source = med.data_source
        and enc.claim_id = med.claim_id
    where encounter_type = 'inpatient hospice'

    union all

    -- Inpatient Psych - Professional Claims
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , 'inpatient psych' as encounter_type
        , 'inpatient' as encounter_group
        , 3 as priority_number
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
        , 3 as priority_number
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
        , 4 as priority_number
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
        , 4 as priority_number
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
        , 5 as priority_number
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
        , 5 as priority_number
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
        , 6 as priority_number
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
        , 6 as priority_number
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
        , 7 as priority_number
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
        , 7 as priority_number
    from {{ ref('encounters__int_multi_day__multi_claim_map') }} as enc
        inner join encounters__stg_medical_claim as med
        on enc.data_source = med.data_source
        and enc.claim_id = med.claim_id
    where encounter_type = 'inpatient substance use'

    -- ========================================================================
    -- OFFICE BASED
    -- ========================================================================

    union all

    -- Office-Based
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , encounter_type
        , 'office based' as encounter_group
        , priority_number -- This is derived in the upstream model.
    from {{ ref('encounters__int_office_visits__single_claim_map') }}

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
        , 20 as priority_number
    from {{ ref('encounters__int_single_day__single_claim_map') }}
    where encounter_type = 'urgent care'

    union all

    -- Outpatient Psych
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , 'outpatient psych' as encounter_type
        , 'outpatient' as encounter_group
        , 21 as priority_number
    from {{ ref('encounters__int_single_day__single_claim_map') }}
    where encounter_type = 'outpatient psych'

    union all

    -- Outpatient Rehabilitation
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , 'outpatient rehabilitation' as encounter_type
        , 'outpatient' as encounter_group
        , 22 as priority_number
    from {{ ref('encounters__int_single_day__single_claim_map') }}
    where encounter_type = 'outpatient rehabilitation'

    union all

    -- Ambulatory Surgery Center
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , 'ambulatory surgery center' as encounter_type
        , 'outpatient' as encounter_group
        , 23 as priority_number
    from {{ ref('encounters__int_multi_day__single_claim_map') }}
    where claim_priority = 1
        and encounter_type = 'ambulatory surgery center'

    union all

    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , 'ambulatory surgery center' as encounter_type
        , 'outpatient' as encounter_group
        , 23 as priority_number
    from {{ ref('encounters__int_multi_day__multi_claim_map') }} as enc
        inner join encounters__stg_medical_claim as med
        on enc.data_source = med.data_source
        and enc.claim_id = med.claim_id
    where enc.encounter_type = 'ambulatory surgery center'

    union all

    -- Dialysis
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , encounter_type
        , 'outpatient' as encounter_group
        , 24 as priority_number
    from {{ ref('encounters__int_single_day__single_claim_map') }}
    where  encounter_type = 'dialysis'

    union all

    -- Outpatient Hospice
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , encounter_type
        , 'outpatient' as encounter_group
        , 25 as priority_number
    from {{ ref('encounters__int_single_day__single_claim_map') }}
    where  encounter_type = 'outpatient hospice'

    union all

    -- Home Health
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , encounter_type
        , 'outpatient' as encounter_group
        , 26 as priority_number
    from {{ ref('encounters__int_single_day__single_claim_map') }}
    where  encounter_type = 'home health'

    union all

    -- Outpatient Surgery
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , encounter_type
        , 'outpatient' as encounter_group
        , 27 as priority_number
    from {{ ref('encounters__int_single_day__single_claim_map') }}
    where encounter_type = 'outpatient surgery'


    union all

    -- Outpatient Injections
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , encounter_type
        , 'outpatient' as encounter_group
        , 28 as priority_number
    from {{ ref('encounters__int_single_day__single_claim_map') }}
    where encounter_type = 'outpatient injections'

    union all

    -- Outpatient PT/OT/ST
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , encounter_type
        , 'outpatient' as encounter_group
        , 29 as priority_number
    from {{ ref('encounters__int_single_day__single_claim_map') }}
    where encounter_type = 'outpatient pt/ot/st'

    union all

    -- Outpatient Substance Use
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , encounter_type
        , 'outpatient' as encounter_group
        , 30 as priority_number
    from {{ ref('encounters__int_single_day__single_claim_map') }}
    where encounter_type = 'outpatient substance use'

    union all

    -- Outpatient Radiology
    select medical_claim_sk
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , encounter_type
        , 'outpatient' as encounter_group
        , 31 as priority_number
    from {{ ref('encounters__int_single_day__single_claim_map') }}
    where encounter_type = 'outpatient radiology'

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
    where encounter_type in ('observation', 'outpatient hospital or clinic')

    -- ========================================================================
    -- ORPHANED ENCOUNTERS
    -- These are services that should roll up into another encounter type, but if
    -- we do not have another claim on that day, then they are considered orphaned.
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
    where encounter_type = 'lab'

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
    where encounter_type = 'dme'

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
    where encounter_type = 'ambulance'
)
select medical_claim_sk
    , encounter_id
    , encounter_start_date
    , encounter_end_date
    , encounter_type
    , encounter_group
    , priority_number
from encounter_claims_union