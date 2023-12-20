-- depends_on: {{ ref('data_quality__claims_preprocessing_summary') }}

{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
   )
}}

-- *************************************************
-- This dbt model creates the pharmacy_claim
-- table in core.
-- *************************************************




select
         cast(claim_id as {{ dbt.type_string() }} ) as claim_id
       , cast(claim_line_number as integer ) as claim_line_number
       , cast(patient_id as {{ dbt.type_string() }} ) as patient_id
       , cast(member_id as {{ dbt.type_string() }} ) as member_id
       , cast(payer as {{ dbt.type_string() }} ) as payer
       , cast(plan as {{ dbt.type_string() }} ) as plan
       , cast(prescribing_provider_npi as {{ dbt.type_string() }} ) as prescribing_provider_npi
       , cast(dispensing_provider_npi as {{ dbt.type_string() }} ) as dispensing_provider_npi
       , cast(dispensing_date as date ) as dispensing_date
       , cast(ndc_code as {{ dbt.type_string() }} ) as ndc_code
       , cast(quantity as integer ) as quantity
       , cast(days_supply as integer ) as days_supply
       , cast(refills as integer ) as refills
       , cast(paid_date as date ) as paid_date
       , cast(paid_amount as {{ dbt.type_numeric() }}) as paid_amount
       , cast(allowed_amount as {{ dbt.type_numeric() }} ) as allowed_amount
       , cast(coinsurance_amount as {{ dbt.type_numeric() }} ) as coinsurance_amount
       , cast(copayment_amount as {{ dbt.type_numeric() }} ) as copayment_amount
       , cast(deductible_amount as {{ dbt.type_numeric() }} ) as deductible_amount
       , cast(data_source as {{ dbt.type_string() }} ) as data_source
       , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('normalized_input__pharmacy_claim') }} 
