/* This model unions together claim lines to encounter crosswalk, and assigns them a unqiue encounter type if claims were assigned to multiple encounters
(This can happen a few ways - professional claims assigned to anchor event overlap and assigned to multiple  )
*/
{{ config(
     enabled = (var('claims_enabled', False) | string | lower) == 'true'
   )
}}
{{ tuva_cluster_by(['claim_id', 'claim_line_number']) }}

with cte as (
select claim_id
 , claim_line_number
 , encounter_id
 , 'acute inpatient' as encounter_type
 , 'inpatient' as encounter_group
 , 0 as priority_number
, null as anchor_claim_id
from {{ ref('int_encounter__acute_inpatient_professional_claim') }}
where claim_attribution_number = 1

union all

/* Intentionally bringing in professional claims assigned to inpatient stays in case admit is assigned to ED  */
select claim_id
 , claim_line_number
 , encounter_id
 , 'emergency department' as encounter_type
 , 'outpatient' as encounter_group
 , 1 as priority_number
, null as anchor_claim_id
from {{ ref('int_encounter__acute_inpatient_professional_claim') }}
where claim_attribution_number = 1

union all

select claim_id
 , claim_line_number
 , encounter_id
 , 'emergency department' as encounter_type
 , 'outpatient' as encounter_group
 , 1 as priority_number
, null as anchor_claim_id
from {{ ref('int_encounter__emergency_department_professional_claim') }}
where claim_attribution_number = 1

union all

select claim_id
, claim_line_number
, encounter_id
, 'inpatient psych' as encounter_type
, 'inpatient' as encounter_group
, 2 as priority_number
, null as anchor_claim_id
from {{ ref('int_encounter__inpatient_psychiatric_professional_claim') }}
where claim_attribution_number = 1

union all

select claim_id
, claim_line_number
, encounter_id
, 'inpatient rehabilitation' as encounter_type
, 'inpatient' as encounter_group
, 3 as priority_number
, null as anchor_claim_id
from {{ ref('int_encounter__inpatient_rehab_professional_claim') }}
where claim_attribution_number = 1

union all

select claim_id
, claim_line_number
, encounter_id
, 'inpatient long term acute care' as encounter_type
, 'inpatient' as encounter_group
, 4 as priority_number
, null as anchor_claim_id
from {{ ref('int_encounter__inpatient_long_term_professional_claim') }}
where claim_attribution_number = 1

union all

select claim_id
, claim_line_number
, encounter_id
, 'inpatient skilled nursing' as encounter_type
, 'inpatient' as encounter_group
, 5 as priority_number
, null as anchor_claim_id
from {{ ref('int_encounter__inpatient_skilled_nursing_professional_claim') }}
where claim_attribution_number = 1

union all

select claim_id
, claim_line_number
, encounter_id
, 'inpatient substance use' as encounter_type
, 'inpatient' as encounter_group
, 6 as priority_number
, null as anchor_claim_id
from {{ ref('int_encounter__inpatient_substance_use_professional_claim') }}
where claim_attribution_number = 1

union all

/* Priority of sub office based types from office based group are set within int_encounter__office_visit_type_priority model */
select claim_id
, claim_line_number
, old_encounter_id
, encounter_type
, 'office based' as encounter_group
, 7 as priority_number
, null as anchor_claim_id
from {{ ref('int_encounter__office_visit_claim_line') }}
where encounter_type = 'office visit radiology'

union all

select claim_id
, claim_line_number
, old_encounter_id
, encounter_type
, 'office based' as encounter_group
, 8 as priority_number
, null as anchor_claim_id
from {{ ref('int_encounter__office_visit_claim_line') }}
where encounter_type <> 'office visit radiology'

union all

/* urgent care set at lower priority than ed and inpatient to avoid over flagging urgent care due to variations in billing practices */
select claim_id
, claim_line_number
, old_encounter_id
, 'urgent care' as encounter_type
, 'outpatient' as encounter_group
, 9 as priority_number
, null as anchor_claim_id
from {{ ref('int_encounter__urgent_care_match_claim_to_anchor') }}

union all

select claim_id
, claim_line_number
, old_encounter_id
, 'outpatient psych' as encounter_type
, 'outpatient' as encounter_group
, 10 as priority_number
, null as anchor_claim_id
from {{ ref('int_encounter__outpatient_psychiatric_match_claim_to_anchor') }}

union all

select claim_id
, claim_line_number
, old_encounter_id
, 'outpatient rehabilitation' as encounter_type
, 'outpatient' as encounter_group
, 11 as priority_number
, null as anchor_claim_id
from {{ ref('int_encounter__outpatient_rehab_match_claim_to_anchor') }}

union all

select claim_id
, claim_line_number
, old_encounter_id
, 'ambulatory surgery center' as encounter_type
, 'outpatient' as encounter_group
, 12 as priority_number
, null as anchor_claim_id
from {{ ref('int_encounter__ambulatory_surgery_center_match_claim_to_anchor') }}

union all

select claim_id
, claim_line_number
, old_encounter_id
, 'dialysis' as encounter_type
, 'outpatient' as encounter_group
, 13 as priority_number
, null as anchor_claim_id
from {{ ref('int_encounter__dialysis_match_claim_to_anchor') }}

union all

select claim_id
, claim_line_number
, old_encounter_id
, 'outpatient hospice' as encounter_type
, 'outpatient' as encounter_group
, 14 as priority_number
, null as anchor_claim_id
from {{ ref('int_encounter__outpatient_hospice_match_claim_to_anchor') }}

union all

select claim_id
, claim_line_number
, old_encounter_id
, 'home health' as encounter_type
, 'outpatient' as encounter_group
, 15 as priority_number
, null as anchor_claim_id
from {{ ref('int_encounter__home_health_match_claim_to_anchor') }}

union all

select claim_id
, claim_line_number
, old_encounter_id
, 'outpatient surgery' as encounter_type
, 'outpatient' as encounter_group
, 16 as priority_number
, null as anchor_claim_id
from {{ ref('int_encounter__outpatient_surgery_match_claim_to_anchor') }}

union all

select claim_id
, claim_line_number
, old_encounter_id
, 'outpatient injections' as encounter_type
, 'outpatient' as encounter_group
, 17 as priority_number
, null as anchor_claim_id
from {{ ref('int_encounter__outpatient_injections_match_claim_to_anchor') }}

union all

select claim_id
, claim_line_number
, old_encounter_id
, 'outpatient pt/ot/st' as encounter_type
, 'outpatient' as encounter_group
, 18 as priority_number
, null as anchor_claim_id
from {{ ref('int_encounter__outpatient_therapy_match_claim_to_anchor') }}

union all

select claim_id
, claim_line_number
, old_encounter_id
, 'outpatient substance use' as encounter_type
, 'outpatient' as encounter_group
, 19 as priority_number
, null as anchor_claim_id
from {{ ref('int_encounter__outpatient_substance_use_match_claim_to_anchor') }}

union all

select claim_id
, claim_line_number
, old_encounter_id
, 'outpatient radiology' as encounter_type
, 'outpatient' as encounter_group
, 20 as priority_number
, null as anchor_claim_id
from {{ ref('int_encounter__outpatient_radiology_match_claim_to_anchor') }}

union all

/* Set as lowest outpatient priority "catch all", roll up to more specific encounter type when available */
select claim_id
, claim_line_number
, old_encounter_id
, 'outpatient hospital or clinic' as encounter_type
, 'outpatient' as encounter_group
, 999 as priority_number
, null as anchor_claim_id
from {{ ref('int_encounter__outpatient_hospital_or_clinic_match_claim_to_anchor') }}

union all

select claim_id
, claim_line_number
, encounter_id
, encounter_type
, encounter_group
, priority_number
, anchor_claim_id
from {{ ref('int_encounter__institutional_claim_line') }}

union all

/* orphaned encounters are "last resort". Labs/DME/ambulance should roll up to inpatient/home health/etc. If unable to match, then they get their own encounter*/

select claim_id
, claim_line_number
, old_encounter_id
, 'lab - orphaned' as encounter_type
, 'other' as encounter_group
, 1000000 as priority_number
, null as anchor_claim_id
from {{ ref('int_encounter__lab_match_claim_to_anchor') }}

union all

select claim_id
, claim_line_number
, old_encounter_id
, 'dme - orphaned' as encounter_type
, 'other' as encounter_group
, 1000001 as priority_number
, null as anchor_claim_id
from {{ ref('int_encounter__dme_match_claim_to_anchor') }}

union all

select claim_id
, claim_line_number
, old_encounter_id
, 'ambulance - orphaned' as encounter_type
, 'other' as encounter_group
, 1000002 as priority_number
, null as anchor_claim_id
from {{ ref('int_encounter__ambulance_match_claim_to_anchor') }}

)


select
  claim_id
, claim_line_number
, encounter_id as old_encounter_id
, {{ dbt_utils.generate_surrogate_key(['encounter_type', 'encounter_id']) }} as encounter_id
, encounter_type
, encounter_group
, priority_number
, anchor_claim_id
, row_number() over (partition by claim_id, claim_line_number
order by priority_number, case when claim_id = anchor_claim_id then 1 else 99 end, encounter_type, encounter_id
  /* A claim line can appear twice for one encounter, identical except that one row
     carries an anchor_claim_id and the other does not. Prefer the row that names an
     anchor, then order on it, so attribution_number 1 is the same row every build. */
, case when anchor_claim_id is null then 1 else 0 end, anchor_claim_id) as claim_line_attribution_number
from cte
