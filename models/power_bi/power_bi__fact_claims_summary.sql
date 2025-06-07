{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

select
      year_month
    , service_category_sk
    , paid_date
    , data_source
    , medical_claim_id
    , encounter_id
    , encounter_type_sk
    ,primary_provider_id
    ,specialty
    , primary_diagnosis_code
    , primary_diagnosis_description
    , ccsr_parent_category
    , ccsr_category
    , ccsr_category_description
    , person_id
    , patient_source_key
    , sum(paid_amount) as paid_amount
from {{ ref('aco_analytics__fact_claims') }}
group by
    year_month
    , service_category_sk
    , paid_date
    , data_source
    , medical_claim_id
    , encounter_id
    , encounter_type_sk
        ,primary_provider_id
    ,specialty
    , primary_diagnosis_code
    , primary_diagnosis_description
    , ccsr_parent_category
    , ccsr_category
    , ccsr_category_description
    , person_id
    , patient_source_key