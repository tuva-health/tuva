{{ config(
     enabled = var('claims_enabled', var('tuva_marts_enabled', False)) | as_bool
)}}

with unioned_data as (

  -- Unioning multiple data quality checks, excluding the _loaded_at field
  {{ dbt_utils.union_relations(
      relations=[
          ref('data_quality__medical_claim_claim_line_fields')
        , ref('data_quality__medical_claim_date_checks')
        , ref('data_quality__medical_claim_inst_header_fields')
        , ref('data_quality__medical_claim_patient_id')
        , ref('data_quality__medical_claim_provider_npi')
        , ref('data_quality__pharmacy_claim_date_checks')
        , ref('data_quality__pharmacy_claim_ndc')
        , ref('data_quality__pharmacy_claim_npi')
        , ref('data_quality__pharmacy_claim_prescription_details')
        , ref('data_quality__pharmacy_patient')
        , ref('data_quality__primary_keys')
      ],
      exclude=["_loaded_at"]
  ) }}

)

select
    data_quality_check
  , result_count
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from unioned_data
