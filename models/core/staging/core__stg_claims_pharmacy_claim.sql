{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False)) | as_bool
   )
}}

-- *************************************************
-- This dbt model creates the pharmacy_claim
-- table in core.
-- *************************************************

select
    {{ concat_custom([
        "cast(pharm.claim_id as " ~ dbt.type_string() ~ ")",
        "'-'",
        "cast(pharm.claim_line_number as " ~ dbt.type_string() ~ ")",
        "'-'",
        "cast(pharm.data_source as " ~ dbt.type_string() ~ ")"
         ]) }} as pharmacy_claim_id
       , cast(pharm.claim_id as {{ dbt.type_string() }} ) as claim_id
       , cast(pharm.claim_line_number as integer ) as claim_line_number
       , cast(pharm.person_id as {{ dbt.type_string() }} ) as person_id
       , cast(pharm.member_id as {{ dbt.type_string() }} ) as member_id
       , cast(pharm.payer as {{ dbt.type_string() }} ) as payer
       , pharm.{{ quote_column('plan') }}
       , cast(pharm.prescribing_provider_id as {{ dbt.type_string() }} ) as prescribing_provider_id
       , cast(pharm.prescribing_provider_name as {{ dbt.type_string() }} ) as prescribing_provider_name
       , cast(pharm.dispensing_provider_id as {{ dbt.type_string() }} ) as dispensing_provider_id
       , cast(pharm.dispensing_provider_name as {{ dbt.type_string() }} ) as dispensing_provider_name
       , cast(pharm.dispensing_date as date ) as dispensing_date
       , cast(pharm.ndc_code as {{ dbt.type_string() }} ) as ndc_code
       , cast(pharm.ndc_description as {{ dbt.type_string() }} ) as ndc_description
       , cast(pharm.quantity as integer ) as quantity
       , cast(pharm.days_supply as integer ) as days_supply
       , cast(pharm.refills as integer ) as refills
       , cast(pharm.paid_date as date ) as paid_date
       , cast(pharm.paid_amount as {{ dbt.type_numeric() }}) as paid_amount
       , cast(pharm.allowed_amount as {{ dbt.type_numeric() }} ) as allowed_amount
       , cast(pharm.charge_amount as {{ dbt.type_numeric() }} ) as charge_amount
       , cast(pharm.coinsurance_amount as {{ dbt.type_numeric() }} ) as coinsurance_amount
       , cast(pharm.copayment_amount as {{ dbt.type_numeric() }} ) as copayment_amount
       , cast(pharm.deductible_amount as {{ dbt.type_numeric() }} ) as deductible_amount
       , cast(pharm.in_network_flag as int ) as in_network_flag
       , cast(
       case
           when enroll.claim_id is not null then 1
              else 0
       end as int) as enrollment_flag
       , enroll.member_month_key
       , cast(pharm.data_source as {{ dbt.type_string() }} ) as data_source
       , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('normalized_input__pharmacy_claim') }}  pharm
left join {{ ref('claims_enrollment__flag_rx_claims_with_enrollment') }} as enroll
  on pharm.claim_id = enroll.claim_id
  and pharm.claim_line_number = enroll.claim_line_number
  and pharm.person_id = enroll.person_id
  and pharm.payer = enroll.payer
  and pharm.{{ quote_column('plan') }} = enroll.{{ quote_column('plan') }}
  and pharm.data_source = enroll.data_source