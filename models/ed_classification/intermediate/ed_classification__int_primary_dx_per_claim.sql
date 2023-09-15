/*
Staging model for the input layer:
This contains one row for every unique primary discharge diagnosis in the dataset.
This is also filtered to ED claims.
*/

{{ config(
     enabled = var('ed_classification_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

with ed_claims as (
  select
    claim_id
    , sum(med.paid_amount) as claim_paid_amount_sum
  from {{ ref('ed_classification__stg_medical_claim') }} med
  left join {{ ref('ed_classification__stg_encounter') }} enc
    on med.encounter_id = enc.encounter_id
  where (place_of_service_code = '23' or revenue_center_code in ('0450', '0451', '0452', '0456', '0459', '0981'))
  and (encounter_type <> 'acute inpatient' or encounter_type is null)
  group by claim_id
)

select
   cast(encounter_id as {{ dbt.type_string() }}) as encounter_id
   , cast(claim_id as {{ dbt.type_string() }}) as claim_id
   , cast(patient_id as {{ dbt.type_string() }}) as patient_id
   , cast(normalized_code_type as {{ dbt.type_string() }}) as code_type
   , cast(normalized_code as {{ dbt.type_string() }}) as code
   , cast(condition.normalized_description as {{ dbt.type_string() }}) as description
   , cast(case
            when lower(condition.normalized_description) like '%covid%'
            then condition.normalized_description
            else mapping.ccs_description
           end
          as {{ dbt.type_string() }}) as ccs_description_with_covid
   , recorded_date
   , cast(claim_paid_amount_sum as {{ dbt.type_float() }}) as claim_paid_amount_sum
from {{ ref('ed_classification__stg_condition') }} condition
inner join ed_claims using(claim_id)
left join {{ ref('ed_classification__icd_10_cm_to_ccs') }} mapping 
on condition.normalized_code = mapping.icd_10_cm
where condition_rank = 1
