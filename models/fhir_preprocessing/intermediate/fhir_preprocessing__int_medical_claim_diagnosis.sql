{{ config(
     enabled = var('fhir_preprocessing_enabled',False) | as_bool
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
              and {{ the_tuva_project.length('claim_condition.normalized_code') }} > 3
              then {{ concat_custom([
                    "substring(claim_condition.normalized_code,1,3)",
                    "'.'",
                    "substring(claim_condition.normalized_code,4)"
                    ]) }}
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
{{ the_tuva_project.create_json_object(
    table_ref='staging',
    group_by_col='claim_id',
    object_col_name='eob_diagnosis_list',
    object_col_list=[
        'eob_diagnosis_sequence'
        , 'eob_diagnosis_system'
        , 'eob_diagnosis_code'
        , 'eob_diagnosis_display'
        , 'eob_diagnosis_type_code'
    ]
) }}
