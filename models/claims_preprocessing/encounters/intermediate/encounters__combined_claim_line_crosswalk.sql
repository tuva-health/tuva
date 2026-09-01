/* This model unions together claim lines to encounter crosswalk, and assigns them a unqiue encounter type if claims were assigned to multiple encounters
(This can happen a few ways - professional claims assigned to anchor event overlap and assigned to multiple  )
*/
{{ config(
     enabled = the_tuva_project.tuva_boolean_var('claims_enabled', false)
   )
}}

with undetermined_claim_lines as (

select distinct
      claim_id
    , claim_line_number
    , data_source
    , 1 as is_undetermined
from {{ ref('encounters__stg_medical_claim') }}
where claim_type = 'undetermined'

)

, cte as (
select claim_id
 , claim_line_number
 , data_source
 , encounter_id
 , 'acute inpatient' as encounter_type
 , 'inpatient' as encounter_group
 , 0 as priority_number
, null as anchor_claim_id
from {{ ref('acute_inpatient__prof_claims') }}
where claim_attribution_number = 1

union all

/* Intentionally bringing in professional claims assigned to inpatient stays in case admit is assigned to ED  */
select claim_id
 , claim_line_number
 , data_source
 , encounter_id
 , 'emergency department' as encounter_type
 , 'outpatient' as encounter_group
 , 1 as priority_number
, null as anchor_claim_id
from {{ ref('acute_inpatient__prof_claims') }}
where claim_attribution_number = 1

union all

select claim_id
 , claim_line_number
 , data_source
 , encounter_id
 , 'emergency department' as encounter_type
 , 'outpatient' as encounter_group
 , 1 as priority_number
, null as anchor_claim_id
from {{ ref('emergency_department__prof_claims') }}
where claim_attribution_number = 1

union all

select claim_id
, claim_line_number
, data_source
, encounter_id
, 'inpatient psych' as encounter_type
, 'inpatient' as encounter_group
, 2 as priority_number
, null as anchor_claim_id
from {{ ref('inpatient_psych__prof_claims') }}
where claim_attribution_number = 1

union all

select claim_id
, claim_line_number
, data_source
, encounter_id
, 'inpatient rehabilitation' as encounter_type
, 'inpatient' as encounter_group
, 3 as priority_number
, null as anchor_claim_id
from {{ ref('inpatient_rehab__prof_claims') }}
where claim_attribution_number = 1

union all

select claim_id
, claim_line_number
, data_source
, encounter_id
, 'inpatient long term acute care' as encounter_type
, 'inpatient' as encounter_group
, 4 as priority_number
, null as anchor_claim_id
from {{ ref('inpatient_long_term__prof_claims') }}
where claim_attribution_number = 1

union all

select claim_id
, claim_line_number
, data_source
, encounter_id
, 'inpatient skilled nursing' as encounter_type
, 'inpatient' as encounter_group
, 5 as priority_number
, null as anchor_claim_id
from {{ ref('inpatient_snf__prof_claims') }}
where claim_attribution_number = 1

union all

/* Keep hospice below the existing inpatient priorities but above every
   office-based and outpatient candidate. Priority 7 is intentionally shared
   with office visit radiology so existing published priority values do not
   change; the final row_number tie-break puts inpatient hospice first. */
select claim_id
, claim_line_number
, data_source
, encounter_id
, 'inpatient hospice' as encounter_type
, 'inpatient' as encounter_group
, 7 as priority_number
, null as anchor_claim_id
from {{ ref('inpatient_hospice__prof_claims') }}
where claim_attribution_number = 1

union all

select claim_id
, claim_line_number
, data_source
, encounter_id
, 'inpatient substance use' as encounter_type
, 'inpatient' as encounter_group
, 6 as priority_number
, null as anchor_claim_id
from {{ ref('inpatient_substance_use__prof_claims') }}
where claim_attribution_number = 1

union all

/* Priority of sub office based types from office based group are set within office_visits__int_office_visits_union model */
select claim_id
, claim_line_number
, data_source
, old_encounter_id
, encounter_type
, 'office based' as encounter_group
, 7 as priority_number
, null as anchor_claim_id
from {{ ref('office_visits__int_office_visits_claim_line') }}
where encounter_type = 'office visit radiology'

union all

select claim_id
, claim_line_number
, data_source
, old_encounter_id
, encounter_type
, 'office based' as encounter_group
, 8 as priority_number
, null as anchor_claim_id
from {{ ref('office_visits__int_office_visits_claim_line') }}
where encounter_type <> 'office visit radiology'

union all

/* urgent care set at lower priority than ed and inpatient to avoid over flagging urgent care due to variations in billing practices */
select claim_id
, claim_line_number
, data_source
, old_encounter_id
, 'urgent care' as encounter_type
, 'outpatient' as encounter_group
, 9 as priority_number
, null as anchor_claim_id
from {{ ref('urgent_care__match_claims_to_anchor') }}

union all

select claim_id
, claim_line_number
, data_source
, old_encounter_id
, 'outpatient psych' as encounter_type
, 'outpatient' as encounter_group
, 10 as priority_number
, null as anchor_claim_id
from {{ ref('outpatient_psych__match_claims_to_anchor') }}

union all

select claim_id
, claim_line_number
, data_source
, old_encounter_id
, 'outpatient rehabilitation' as encounter_type
, 'outpatient' as encounter_group
, 11 as priority_number
, null as anchor_claim_id
from {{ ref('outpatient_rehab__match_claims_to_anchor') }}

union all

select claim_id
, claim_line_number
, data_source
, old_encounter_id
, 'ambulatory surgery center' as encounter_type
, 'outpatient' as encounter_group
, 12 as priority_number
, null as anchor_claim_id
from {{ ref('asc__match_claims_to_anchor') }}

union all

select claim_id
, claim_line_number
, data_source
, old_encounter_id
, 'dialysis' as encounter_type
, 'outpatient' as encounter_group
, 13 as priority_number
, null as anchor_claim_id
from {{ ref('dialysis__match_claims_to_anchor') }}

union all

select claim_id
, claim_line_number
, data_source
, old_encounter_id
, 'outpatient hospice' as encounter_type
, 'outpatient' as encounter_group
, 14 as priority_number
, null as anchor_claim_id
from {{ ref('outpatient_hospice__match_claims_to_anchor') }}

union all

select claim_id
, claim_line_number
, data_source
, old_encounter_id
, 'home health' as encounter_type
, 'outpatient' as encounter_group
, 15 as priority_number
, null as anchor_claim_id
from {{ ref('home_health__match_claims_to_anchor') }}

union all

select claim_id
, claim_line_number
, data_source
, old_encounter_id
, 'outpatient surgery' as encounter_type
, 'outpatient' as encounter_group
, 16 as priority_number
, null as anchor_claim_id
from {{ ref('outpatient_surgery__match_claims_to_anchor') }}

union all

select claim_id
, claim_line_number
, data_source
, old_encounter_id
, 'outpatient injections' as encounter_type
, 'outpatient' as encounter_group
, 17 as priority_number
, null as anchor_claim_id
from {{ ref('outpatient_injections__match_claims_to_anchor') }}

union all

select claim_id
, claim_line_number
, data_source
, old_encounter_id
, 'outpatient pt/ot/st' as encounter_type
, 'outpatient' as encounter_group
, 18 as priority_number
, null as anchor_claim_id
from {{ ref('outpatient_ptotst__match_claims_to_anchor') }}

union all

select claim_id
, claim_line_number
, data_source
, old_encounter_id
, 'outpatient substance use' as encounter_type
, 'outpatient' as encounter_group
, 19 as priority_number
, null as anchor_claim_id
from {{ ref('outpatient_substance_use__match_claims_to_anchor') }}

union all

select claim_id
, claim_line_number
, data_source
, old_encounter_id
, 'outpatient radiology' as encounter_type
, 'outpatient' as encounter_group
, 20 as priority_number
, null as anchor_claim_id
from {{ ref('outpatient_radiology__match_claims_to_anchor') }}

union all

/* Set as lowest outpatient priority "catch all", roll up to more specific encounter type when available */
select claim_id
, claim_line_number
, data_source
, old_encounter_id
, 'outpatient hospital or clinic' as encounter_type
, 'outpatient' as encounter_group
, 999 as priority_number
, null as anchor_claim_id
from {{ ref('outpatient_hospital_or_clinic__match_claims_to_anchor') }}

union all

select claim_id
, claim_line_number
, data_source
, encounter_id
, encounter_type
, encounter_group
, priority_number
, anchor_claim_id
from {{ ref('encounters__int_institutional_claim_lines') }}

union all

/* orphaned encounters are "last resort". Labs/DME/ambulance should roll up to inpatient/home health/etc. If unable to match, then they get their own encounter*/

select claim_id
, claim_line_number
, data_source
, old_encounter_id
, 'lab - orphaned' as encounter_type
, 'other' as encounter_group
, 1000000 as priority_number
, null as anchor_claim_id
from {{ ref('lab__match_claims_to_anchor') }}

union all

select claim_id
, claim_line_number
, data_source
, old_encounter_id
, 'dme - orphaned' as encounter_type
, 'other' as encounter_group
, 1000001 as priority_number
, null as anchor_claim_id
from {{ ref('dme__match_claims_to_anchor') }}

union all

select claim_id
, claim_line_number
, data_source
, old_encounter_id
, 'ambulance - orphaned' as encounter_type
, 'other' as encounter_group
, 1000002 as priority_number
, null as anchor_claim_id
from {{ ref('ambulance__match_claims_to_anchor') }}

)


select
  candidate.claim_id
, candidate.claim_line_number
, candidate.data_source
, candidate.encounter_id as old_encounter_id
, {{ the_tuva_project.encounter_id_hash(['candidate.encounter_type', 'candidate.encounter_id']) }} as encounter_id
, candidate.encounter_type
, candidate.encounter_group
, candidate.priority_number
, candidate.anchor_claim_id
, row_number() over (partition by candidate.claim_id, candidate.claim_line_number, candidate.data_source
order by candidate.priority_number
       , case when candidate.encounter_type = 'inpatient hospice' then 0 else 1 end
       , case when candidate.claim_id = candidate.anchor_claim_id then 1 else 99 end
       , candidate.encounter_type, candidate.encounter_id) as claim_line_attribution_number
from cte as candidate
left outer join undetermined_claim_lines
    on candidate.claim_id = undetermined_claim_lines.claim_id
    and candidate.claim_line_number = undetermined_claim_lines.claim_line_number
    and candidate.data_source = undetermined_claim_lines.data_source
where undetermined_claim_lines.is_undetermined is null
