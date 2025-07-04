select
    pharm.pharmacy_claim_sk
    , pharm.data_source
    , pharm.claim_id
    , pharm.claim_line_number
    , pharm.person_id
    , pharm.member_id
    , pharm.payer
    , pharm.{{ quote_column('plan') }}
    , pharm.prescribing_provider_npi
    , pres.provider_name as prescribing_provider_name
    , pharm.dispensing_provider_npi
    , disp.provider_name as dispensing_provider_name
    , pharm.dispensing_date
    , pharm.ndc_code
    , cast(null as {{ dbt.type_string() }}) as ndc_description
    , pharm.quantity
    , pharm.days_supply
    , pharm.refills
    , pharm.paid_date
    , pharm.paid_amount
    , pharm.allowed_amount
    , pharm.charge_amount
    , pharm.coinsurance_amount
    , pharm.copayment_amount
    , pharm.deductible_amount
    , pharm.in_network_flag
    , pharm.file_name
    , pharm.file_date
    , pharm.ingest_datetime
from {{ ref('the_tuva_project', 'normalized_input__pharmacy_claim') }} as pharm
left outer join {{ ref('tuva_data_assets', 'npi') }} as pres
      on pharm.prescribing_provider_npi = pres.npi
left outer join {{ ref('tuva_data_assets', 'npi') }} as disp
      on pharm.dispensing_provider_npi = disp.npi
