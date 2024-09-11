{{ config(
    enabled = var('claims_enabled', False)
) }}

with unioned_data as (
    {{ dbt_utils.union_relations(
        relations=[
            ref('data_quality__claim_allowed_amount')
          , ref('data_quality__claim_charge_amount')
          , ref('data_quality__claim_claim_id')
          , ref('data_quality__claim_claim_line_end_date')
          , ref('data_quality__claim_claim_line_number')
          , ref('data_quality__claim_claim_line_start_date')
          , ref('data_quality__claim_claim_type')
          , ref('data_quality__claim_coinsurance_amount')
          , ref('data_quality__claim_copayment_amount')
          , ref('data_quality__claim_data_source')
          , ref('data_quality__claim_deductible_amount')
          , ref('data_quality__claim_diagnosis_code_type')
          , ref('data_quality__claim_hcpcs_code')
          , ref('data_quality__claim_member_id')
          , ref('data_quality__claim_paid_amount')
          , ref('data_quality__claim_paid_date')
          , ref('data_quality__claim_patient_id')
          , ref('data_quality__claim_payer')
          , ref('data_quality__claim_plan')
          , ref('data_quality__claim_total_cost_amount')
          , ref('data_quality__eligibility_address')
          , ref('data_quality__eligibility_birth_date')
          , ref('data_quality__eligibility_city')
          , ref('data_quality__eligibility_data_source')
          , ref('data_quality__eligibility_death_date')
          , ref('data_quality__eligibility_death_flag')
          , ref('data_quality__eligibility_dual_status_code')
          , ref('data_quality__eligibility_end_date')
          , ref('data_quality__eligibility_first_name')
          , ref('data_quality__eligibility_gender')
          , ref('data_quality__eligibility_last_name')
          , ref('data_quality__eligibility_medicare_status_code')
          , ref('data_quality__eligibility_member_id')
          , ref('data_quality__eligibility_original_reason_entitlement_code')
          , ref('data_quality__eligibility_patient_id')
          , ref('data_quality__eligibility_payer_type')
          , ref('data_quality__eligibility_payer')
          , ref('data_quality__eligibility_phone')
          , ref('data_quality__eligibility_plan')
          , ref('data_quality__eligibility_race')
          , ref('data_quality__eligibility_start_date')
          , ref('data_quality__eligibility_state')
          , ref('data_quality__eligibility_zip_code')
          , ref('data_quality__institutional_ms_drg_code')
          , ref('data_quality__institutional_admission_date')
          , ref('data_quality__institutional_admit_source_code')
          , ref('data_quality__institutional_admit_type_code')
          , ref('data_quality__institutional_apr_drg_code')
          , ref('data_quality__institutional_bill_type_code')
          , ref('data_quality__institutional_billing_npi')
          , ref('data_quality__institutional_claim_end_date')
          , ref('data_quality__institutional_claim_start_date')
          , ref('data_quality__institutional_diagnosis_code_1')
          , ref('data_quality__institutional_diagnosis_code_2')
          , ref('data_quality__institutional_diagnosis_code_3')
          , ref('data_quality__institutional_discharge_date')
          , ref('data_quality__institutional_discharge_disposition_code')
          , ref('data_quality__institutional_facility_npi')
          , ref('data_quality__institutional_present_on_admission_1')
          , ref('data_quality__institutional_present_on_admission_2')
          , ref('data_quality__institutional_present_on_admission_3')
          , ref('data_quality__institutional_procedure_code_1')
          , ref('data_quality__institutional_procedure_code_2')
          , ref('data_quality__institutional_procedure_code_3')
          , ref('data_quality__institutional_procedure_date_1')
          , ref('data_quality__institutional_procedure_date_2')
          , ref('data_quality__institutional_procedure_date_3')
          , ref('data_quality__institutional_rendering_npi')
          , ref('data_quality__institutional_revenue_center_code')
          , ref('data_quality__institutional_service_unit_quantity')
          , ref('data_quality__pharmacy_allowed_amount')
          , ref('data_quality__pharmacy_claim_id')
          , ref('data_quality__pharmacy_claim_line_number')
          , ref('data_quality__pharmacy_coinsurance_amount')
          , ref('data_quality__pharmacy_copayment_amount')
          , ref('data_quality__pharmacy_data_source')
          , ref('data_quality__pharmacy_days_supply')
          , ref('data_quality__pharmacy_deductible_amount')
          , ref('data_quality__pharmacy_dispensing_date')
          , ref('data_quality__pharmacy_dispensing_provider_npi')
          , ref('data_quality__pharmacy_member_id')
          , ref('data_quality__pharmacy_ndc_code')
          , ref('data_quality__pharmacy_paid_amount')
          , ref('data_quality__pharmacy_paid_date')
          , ref('data_quality__pharmacy_patient_id')
          , ref('data_quality__pharmacy_payer')
          , ref('data_quality__pharmacy_plan')
          , ref('data_quality__pharmacy_prescribing_provider_npi')
          , ref('data_quality__pharmacy_quantity')
          , ref('data_quality__pharmacy_refills')
          , ref('data_quality__professional_billing_npi')
          , ref('data_quality__professional_facility_npi')
          , ref('data_quality__professional_place_of_service_code')
          , ref('data_quality__professional_rendering_npi')
          , ref('data_quality__professional_claim_end_date')
          , ref('data_quality__professional_claim_start_date')
          , ref('data_quality__professional_diagnosis_code_1')
          , ref('data_quality__professional_diagnosis_code_2')
          , ref('data_quality__professional_diagnosis_code_3')
        ],
        exclude=["_loaded_at"]
    ) }}
)

select
    cast(data_source as {{ dbt.type_string() }}) as data_source
  , cast(source_date as {{ dbt.type_string() }}) as source_date
  , cast(table_name as {{ dbt.type_string() }}) as table_name
  , cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
  , cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
  , cast(claim_type as {{ dbt.type_string() }}) as claim_type
  , cast(field_name as {{ dbt.type_string() }}) as field_name
  , cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
  , cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
  , cast(field_value as {{ dbt.type_string() }}) as field_value
  , cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
  , dense_rank() over (
        order by data_source
               , table_name
               , claim_type
               , field_name
    ) as summary_sk
from unioned_data
