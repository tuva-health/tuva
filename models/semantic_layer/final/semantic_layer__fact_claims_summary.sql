{{ config(
     enabled = var('semantic_layer_enabled',False) | as_bool
   )
}}


select
      year_month
    , service_category_sk
    , {{ dbt.concat(["person_id", "'|'", "year_month"]) }} as member_month_sk
    , paid_date
    , data_source
    , medical_claim_id
    , encounter_id
    , encounter_type_sk
    , primary_provider_id
    , specialty
    , primary_diagnosis_code
    , primary_diagnosis_description
    , ccsr_parent_category
    , ccsr_category
    , ccsr_category_description
    , person_id
    , patient_source_key
    , '{{ var('tuva_last_run') }}' as tuva_last_run
    , min(claim_start_date) as claim_start_date
    , max(claim_end_date) as claim_end_date
    , sum(paid_amount) as paid_amount
from {{ ref('semantic_layer__fact_claims') }}
group by
      year_month
    , service_category_sk
    , paid_date
    , data_source
    , medical_claim_id
    , encounter_id
    , encounter_type_sk
    , primary_provider_id
    , specialty
    , primary_diagnosis_code
    , primary_diagnosis_description
    , ccsr_parent_category
    , ccsr_category
    , ccsr_category_description
    , person_id
    , patient_source_key
    , '{{ var('tuva_last_run') }}' as tuva_last_run