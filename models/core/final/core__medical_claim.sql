{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

{%- set tuva_core_columns -%}
      medical_claim_id
    , claim_id
    , claim_line_number
    , encounter_id
    , encounter_type
    , encounter_group
    , claim_type
    , person_id
    , member_id
    , payer
    , {{ quote_column('plan') }}
    , claim_start_date
    , claim_end_date
    , claim_line_start_date
    , claim_line_end_date
    , admission_date
    , discharge_date
    , service_category_1
    , service_category_2
    , service_category_3
    , admit_source_code
    , admit_source_description
    , admit_type_code
    , admit_type_description
    , discharge_disposition_code
    , discharge_disposition_description
    , place_of_service_code
    , place_of_service_description
    , bill_type_code
    , bill_type_description
    , drg_code_type
    , drg_code
    , drg_description
    , revenue_center_code
    , revenue_center_description
    , service_unit_quantity
    , hcpcs_code
    , hcpcs_modifier_1
    , hcpcs_modifier_2
    , hcpcs_modifier_3
    , hcpcs_modifier_4
    , hcpcs_modifier_5
    , rendering_id
    , rendering_tin
    , rendering_name
    , billing_id
    , billing_tin
    , billing_name
    , facility_id
    , facility_name
    , paid_date
    , paid_amount
    , allowed_amount
    , charge_amount
    , coinsurance_amount
    , copayment_amount
    , deductible_amount
    , total_cost_amount
    , in_network_flag
    , enrollment_flag
    , member_month_key
{%- endset -%}

{%- set tuva_extension_columns -%}
    {{ select_extension_columns(ref('input_layer__medical_claim')) }}
{%- endset -%}

{%- set tuva_metadata_columns -%}
    , data_source
    , file_date
    , file_name
    , tuva_last_run
{%- endset %}

select
    {{ tuva_core_columns }}
    {{ tuva_extension_columns }}
    {{ tuva_metadata_columns }}
from {{ ref('core__stg_claims_medical_claim') }}
