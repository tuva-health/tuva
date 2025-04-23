{{ config(
     enabled = var('fhir_preprocessing_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with staging as (

    select
          medical_claim.claim_id
        , claim_condition.condition_rank as eob_diagnosis_sequence
        , case
            when lower(claim_condition.normalized_code_type) = 'icd-10-cm' then 'ICD10'
            else 'ICD9'
          end as eob_diagnosis_system
        /* HEDIS valuesets require formatted diagnosis codes */
        , case
            when lower(claim_condition.normalized_code_type) = 'icd-10-cm'
              and len(claim_condition.normalized_code) > 3
              then substr(claim_condition.normalized_code,1,3)
                || '.'
                || substr(claim_condition.normalized_code,4)
            else claim_condition.normalized_code
          end as eob_diagnosis_code
        , replace(claim_condition.normalized_description,',','') as eob_diagnosis_display
        , case
            when claim_condition.condition_rank = 1 then 'principal'
            else 'other'
          end as eob_diagnosis_type_code
    from {{ ref('fhir_preprocessing__stg_core__medical_claim') }} as medical_claim
        inner join {{ ref('fhir_preprocessing__stg_core__condition') }} as claim_condition
            on medical_claim.claim_id = claim_condition.claim_id
    where medical_claim.claim_line_number = 1 /* filter to claim header */

)

/* create a json string for CSV export */
select
      claim_id
    , to_json(
        array_agg(
            object_construct(
                  'eobDiagnosisSequence', eob_diagnosis_sequence
                , 'eobDiagnosisSystem', eob_diagnosis_system
                , 'eobDiagnosisCode', eob_diagnosis_code
                , 'eobDiagnosisDisplay', eob_diagnosis_display
                , 'eobDiagnosisTypeCode', eob_diagnosis_type_code
            )
        ) within group (order by eob_diagnosis_sequence)
      ) as eob_diagnosis_list
from staging
group by claim_id
