{{ config(
     enabled = var('readmissions_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

-- Here we list encounter_ids for all encounters
-- that are planned.


-- encounter_ids for encounters that we know
-- are planned because they had a procedure category
-- that is only present for planned encounters 
with always_planned_px as (
select distinct pccs.encounter_id
from {{ ref('readmissions__procedure_ccs') }} as pccs
inner join {{ ref('readmissions__always_planned_ccs_procedure_category') }} as apc
    on pccs.ccs_procedure_category = apc.ccs_procedure_category
)


-- encounter_ids for encounters that we know
-- are planned because they had a diagnosis category
-- that is only present for planned encounters
, always_planned_dx as (
select distinct encounter_id
from {{ ref('readmissions__encounter_with_ccs') }} as dccs
inner join {{ ref('readmissions__always_planned_ccs_diagnosis_category') }} as apd
    on dccs.ccs_diagnosis_category = apd.ccs_diagnosis_category
)


-- encounter_ids for encounters that are potentially planned
-- based on one of their CCS procedure categories.
-- For these encounters to actually be planned, we must further
-- require that they are NOT acute encounters
, potentially_planned_px_ccs as (
select distinct encounter_id
from {{ ref('readmissions__procedure_ccs') }} as pccs
inner join {{ ref('readmissions__potentially_planned_ccs_procedure_category') }} as pcs
    on pccs.ccs_procedure_category = pcs.ccs_procedure_category
)


-- encounter_ids for encounters that are potentially planned
-- based on their ICD-10-PCS procedure codes.
-- For these encounters to actually be planned, we must further
-- require that they are NOT acute encounters
, potentially_planned_px_icd_10_pcs as (
select distinct encounter_id
from {{ ref('readmissions__procedure_ccs') }} as pcs
inner join {{ ref('readmissions__potentially_planned_icd_10_pcs') }} as pps
    on pcs.procedure_code = pps.icd_10_pcs
)


-- encounter_ids for encounters that are acute based
-- on their primary diagnosis code or their CCS diagnosis category
, acute_encounters as (
select distinct encounter_id
from {{ ref('readmissions__encounter_with_ccs') }} as dccs
left outer join {{ ref('readmissions__acute_diagnosis_icd_10_cm') }} as adi
    on dccs.primary_diagnosis_code = adi.icd_10_cm
left outer join {{ ref('readmissions__acute_diagnosis_ccs') }} as adc
    on dccs.ccs_diagnosis_category = adc.ccs_diagnosis_category
where adi.icd_10_cm is not null or adc.ccs_diagnosis_category is not null
)


-- encounter_ids for encounters that are:
--           [1] potentially planned, based on one of
--               their CCS procedure categories or
--               their ICD-10-PCS procedure codes
--           [2] not acute, based on their primary diagnosis code
--               or their CCS diagnosis category
-- These encounters are therefore confirmed to be planned
, potentially_planned_that_are_actually_planned as (
select distinct ppp.encounter_id
from (
    select * from potentially_planned_px_ccs
        union all
    select * from potentially_planned_px_icd_10_pcs) as ppp
left outer join acute_encounters
    on ppp.encounter_id = acute_encounters.encounter_id
where acute_encounters.encounter_id is null

)


-- Aggregate of all encounter_ids for planned encounters

select *, '{{ var('tuva_last_run') }}' as tuva_last_run from always_planned_px
{% if target.type == 'fabric' %}
union
{% else %}
union distinct
{% endif %}
select *, '{{ var('tuva_last_run') }}' as tuva_last_run from always_planned_dx
{% if target.type == 'fabric' %}
union
{% else %}
union distinct
{% endif %}
select *, '{{ var('tuva_last_run') }}' as tuva_last_run from potentially_planned_that_are_actually_planned
