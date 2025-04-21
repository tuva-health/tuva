{{ config(
     enabled = var('fhir_preprocessing_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with add_sequence as (

    select
          claim_id
        , procedure_id
        , row_number() over(
            partition by claim_id
            order by procedure_id
          ) as eob_procedure_sequence
    from {{ ref('fhir_preprocessing__stg_core__procedure') }}

)

, staging as (

    select
          medical_claim.claim_id
        , add_sequence.eob_procedure_sequence
        , case
            when lower(claim_procedure.normalized_code_type) = 'icd-10-pcs' then 'ICD10PCS'
            else 'ICD9'
          end as eob_procedure_system
        , claim_procedure.normalized_code as eob_procedure_code
        , replace(claim_procedure.normalized_description,',','') as eob_procedure_display
        , case
            when add_sequence.eob_procedure_sequence = 1 then 'principal'
            else 'other'
          end as eob_procedure_type_code
    from {{ ref('fhir_preprocessing__stg_core__medical_claim') }} as medical_claim
        inner join {{ ref('fhir_preprocessing__stg_core__procedure') }} as claim_procedure
            on medical_claim.claim_id = claim_procedure.claim_id
        inner join add_sequence
            on claim_procedure.claim_id = add_sequence.claim_id
            and claim_procedure.procedure_id = add_sequence.procedure_id
    where medical_claim.claim_line_number = 1 /* filter to claim header */
    and lower(claim_procedure.normalized_code_type) in ('icd-9-pcs', 'icd-10-pcs')

)

/* create a json string for CSV export */
select
      claim_id
    , to_json(
        array_agg(
            object_construct(
                  'eob_procedure_sequence', eob_procedure_sequence
                , 'eob_procedure_system', eob_procedure_system
                , 'eob_procedure_code', eob_procedure_code
                , 'eob_procedure_display', eob_procedure_display
                , 'eob_procedure_type_code', eob_procedure_type_code
            )
        ) within group (order by eob_procedure_sequence)
      ) as eob_procedure_list
from staging
group by claim_id
