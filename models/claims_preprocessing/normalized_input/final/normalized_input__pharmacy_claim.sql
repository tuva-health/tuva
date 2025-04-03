{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
 | as_bool
   )
}}


select
      cast(claim_id as {{ dbt.type_string() }} ) as claim_id
    , cast(claim_line_number as int ) as claim_line_number
    , cast(person_id as {{ dbt.type_string() }} ) as person_id
    , cast(member_id as {{ dbt.type_string() }} ) as member_id
    , cast(payer as {{ dbt.type_string() }} ) as payer
    , cast({{ quote_column('plan') }} as {{ dbt.type_string() }} ) as {{ quote_column('plan') }}
    , cast(prescribing_provider_npi as {{ dbt.type_string() }} ) as prescribing_provider_id
    , cast(coalesce({{ concat_custom(["pres.provider_last_name", "', '", "pres.provider_first_name"]) }}, pres.provider_organization_name) as {{ dbt.type_string() }} ) as prescribing_provider_name
    , cast(dispensing_provider_npi as {{ dbt.type_string() }} ) as dispensing_provider_id
    , cast(coalesce({{ concat_custom(["disp.provider_last_name", "', '", "disp.provider_first_name"]) }}, disp.provider_organization_name) as {{ dbt.type_string() }} ) as dispensing_provider_name    , cast(dispensing_date as date ) as dispensing_date
    , cast(ndc_code as {{ dbt.type_string() }} ) as ndc_code
    , cast(ndc.fda_description as {{ dbt.type_string() }} ) as ndc_description
    , cast(quantity as int ) as quantity
    , cast(days_supply as int ) as days_supply
    , cast(refills as int ) as refills
    , cast(paid_date as date ) as paid_date
    , cast(paid_amount as {{ dbt.type_numeric() }} ) as paid_amount
    , cast(allowed_amount as {{ dbt.type_numeric() }} ) as allowed_amount
    , cast(charge_amount as {{ dbt.type_numeric() }} ) as charge_amount
    , cast(coinsurance_amount as {{ dbt.type_numeric() }} ) as coinsurance_amount
    , cast(copayment_amount as {{ dbt.type_numeric() }} ) as copayment_amount
    , cast(deductible_amount as {{ dbt.type_numeric() }} ) as deductible_amount
    , cast(in_network_flag as int ) as in_network_flag
    , cast(data_source as {{ dbt.type_string() }} ) as data_source
    , cast('{{ var('tuva_last_run')}}' as {{ dbt.type_string() }} ) as tuva_last_run
from {{ ref('normalized_input__stg_pharmacy_claim') }} pharm
left join {{ ref('terminology__provider') }} pres
      on pharm.prescribing_provider_npi = pres.npi
left join {{ ref('terminology__provider') }} disp
      on pharm.dispensing_provider_npi = disp.npi
left join {{ ref('terminology__ndc') }} ndc
      on pharm.ndc_code = ndc.ndc
