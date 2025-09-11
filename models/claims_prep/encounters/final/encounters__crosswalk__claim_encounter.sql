-- ============================================================================
-- ENCOUNTER CLAIMS CROSSWALK
-- ============================================================================
-- This model creates a comprehensive crosswalk between claims and encounters across
-- all encounter types. When claims are assigned to multiple encounters, priority
-- numbers determine the final assignment (lower number = higher priority).

with union_crosswalk as (
    {{ dbt_utils.union_relations(
        relations=[ref('encounters__int_crosswalk_claim_assigned'), ref('encounters__int_crosswalk_claim_orphaned')],
        exclude=[""]
    ) }}
)
select medical_claim_sk
    , encounter_id
    , encounter_start_date
    , encounter_end_date
    , -- Create new sequential encounter IDs TODO: Change to a hash for a consistent value, and to support incremental processing
    dense_rank() over (order by encounter_id) as encounter_sk
    , encounter_type
    , encounter_group
    , priority_number
    , -- Get a deterministic assignment per medical_claim_sk
    row_number() over (partition by medical_claim_sk order by priority_number) as encounter_type_priority
from union_crosswalk