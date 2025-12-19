{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

with unioned as (
    select enc.claim_id
, enc.encounter_id
, 'acute inpatient' as encounter_type
, 'inpatient' as encounter_group
, 0 as priority_number
, null as anchor_claim_id
from {{ ref('acute_inpatient__generate_encounter_id') }} as enc


union all

select enc.claim_id
, enc.encounter_id
, 'emergency department' as encounter_type
, 'outpatient' as encounter_group
, 1 as priority_number
, original_anchor_claim as anchor_claim_id
from {{ ref('emergency_department__generate_encounter_id') }} as enc


union all

select enc.claim_id
, enc.encounter_id
, 'inpatient hospice' as encounter_type
, 'inpatient' as encounter_group
, 1 as priority_number
, null as anchor_claim_id
from {{ ref('inpatient_hospice__generate_encounter_id') }} as enc


union all

select enc.claim_id
, enc.encounter_id
, 'inpatient psych' as encounter_type
, 'inpatient' as encounter_group
, 2 as priority_number
, null as anchor_claim_id
from {{ ref('inpatient_psych__generate_encounter_id') }} as enc


union all

select enc.claim_id
, enc.encounter_id
, 'inpatient rehabilitation' as encounter_type
, 'inpatient' as encounter_group
, 3 as priority_number
, null as anchor_claim_id
from {{ ref('inpatient_rehab__generate_encounter_id') }} as enc


union all

select enc.claim_id
, enc.encounter_id
, 'inpatient long term acute care' as encounter_type
, 'inpatient' as encounter_group
, 4 as priority_number
, null as anchor_claim_id
from {{ ref('inpatient_long_term__generate_encounter_id') }} as enc



union all

select enc.claim_id
, enc.encounter_id
, 'inpatient skilled nursing' as encounter_type
, 'inpatient' as encounter_group
, 5 as priority_number
, null as anchor_claim_id
from {{ ref('inpatient_snf__generate_encounter_id') }} as enc


union all

select enc.claim_id
, enc.encounter_id
, 'inpatient substance use' as encounter_type
, 'inpatient' as encounter_group
, 6 as priority_number
, null as anchor_claim_id
from {{ ref('inpatient_substance_use__generate_encounter_id') }} as enc

)

, final as (
    select
        enc.claim_id
        , med.claim_line_number
        , enc.encounter_id
        , 'inpatient substance use' as encounter_type
        , 'inpatient' as encounter_group
        , 6 as priority_number
        , null as anchor_claim_id
    from unioned as enc
    inner join {{ ref('encounters__stg_medical_claim') }} as med on enc.claim_id = med.claim_id
)

select * from final
