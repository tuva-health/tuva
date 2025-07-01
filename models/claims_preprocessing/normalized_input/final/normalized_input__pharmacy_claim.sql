select
    pharm.surrogate_key
    , cast(pharm.claim_id as {{ dbt.type_string() }}) as claim_id
    , cast(pharm.claim_line_number as int) as claim_line_number
    , cast(pharm.person_id as {{ dbt.type_string() }}) as person_id
    , cast(pharm.member_id as {{ dbt.type_string() }}) as member_id
    , cast(pharm.payer as {{ dbt.type_string() }}) as payer
    , cast({{ quote_column('plan') }} as {{ dbt.type_string() }}) as {{ quote_column('plan') }}
    , cast(pharm.prescribing_provider_npi as {{ dbt.type_string() }}) as prescribing_provider_id
    , cast(pres.provider_name as {{ dbt.type_string() }}) as prescribing_provider_name
    , cast(pharm.dispensing_provider_npi as {{ dbt.type_string() }}) as dispensing_provider_id
    , cast(disp.provider_name as {{ dbt.type_string() }}) as dispensing_provider_name
    , cast(pharm.dispensing_date as date) as dispensing_date
    , cast(pharm.ndc_code as {{ dbt.type_string() }}) as ndc_code
    , cast(null as {{ dbt.type_string() }}) as ndc_description
    , cast(pharm.quantity as int) as quantity
    , cast(pharm.days_supply as int) as days_supply
    , cast(pharm.refills as int) as refills
    , cast(pharm.paid_date as date) as paid_date
    , cast(pharm.paid_amount as {{ dbt.type_numeric() }}) as paid_amount
    , cast(pharm.allowed_amount as {{ dbt.type_numeric() }}) as allowed_amount
    , cast(pharm.charge_amount as {{ dbt.type_numeric() }}) as charge_amount
    , cast(pharm.coinsurance_amount as {{ dbt.type_numeric() }}) as coinsurance_amount
    , cast(pharm.copayment_amount as {{ dbt.type_numeric() }}) as copayment_amount
    , cast(pharm.deductible_amount as {{ dbt.type_numeric() }}) as deductible_amount
    , cast(pharm.in_network_flag as int) as in_network_flag
    , cast(pharm.data_source as {{ dbt.type_string() }}) as data_source
from {{ ref('the_tuva_project', 'normalized_input__stg_pharmacy_claim') }} as pharm
left outer join {{ ref('tuva_data_assets', 'npi') }} as pres
      on pharm.prescribing_provider_npi = pres.npi
left outer join {{ ref('tuva_data_assets', 'npi') }} as disp
      on pharm.dispensing_provider_npi = disp.npi
