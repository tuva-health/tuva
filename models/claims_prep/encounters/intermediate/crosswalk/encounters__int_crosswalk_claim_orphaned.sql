-- ============================================================================
-- ORPHAN CLAIMS CROSSWALK
-- ============================================================================
-- This model creates a crosswalk between claims and encounters where an
-- encounter type was not able to be determined. These are listed as "orphaned claim".
with encounters__stg_medical_claim as (
    select *
    from {{ ref('encounters__stg_medical_claim') }}
),
encounters__int_crosswalk_claim_assigned as (
    select *
    from {{ ref('encounters__int_crosswalk_claim_assigned') }}
)
-- Produce an encounter for claims that did not get grouped.
select
    med.medical_claim_sk
    , claim_id as encounter_id
    , claim_start_date as encounter_start_date
    , claim_end_date as encounter_end_date
    , {{ dbt_utils.generate_surrogate_key(['patient_sk', 'start_date', 'facility_npi']) }} as encounter_sk
    , 'orphaned claim' as encounter_type
    , 'other' as encounter_group
    , 9999 as priority_number
from encounters__stg_medical_claim as med
    left outer join encounters__int_crosswalk_claim_assigned as enc
    on med.medical_claim_sk = enc.medical_claim_sk
where enc.medical_claim_sk is null