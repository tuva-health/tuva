{{ config(
     enabled = the_tuva_project.tuva_boolean_var('claims_enabled', false)
   )
}}


select distinct
  med.claim_id
  , med.claim_line_number
  , med.claim_type
  , med.data_source
  , med.claim_start_date as normalized_claim_start_date
  , med.claim_end_date as normalized_claim_end_date
  , med.claim_line_start_date as normalized_claim_line_start_date
  , med.claim_line_end_date as normalized_claim_line_end_date
  , med.admission_date as normalized_admission_date
  , med.discharge_date as normalized_discharge_date
  , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('normalized_input__stg_medical_claim') }} as med
