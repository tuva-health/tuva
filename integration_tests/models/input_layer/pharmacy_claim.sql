select
    cast(claim_id as {{ dbt.type_string() }}) as claim_id
    , cast(claim_line_number as {{ dbt.type_int() }}) as claim_line_number
    , cast(person_id as {{ dbt.type_string() }}) as person_id
    , cast(member_id as {{ dbt.type_string() }}) as member_id
    , cast(payer as {{ dbt.type_string() }}) as payer
    , cast(plan as {{ dbt.type_string() }}) as plan
    , cast(prescribing_provider_npi as {{ dbt.type_string() }}) as prescribing_provider_npi
    , cast(dispensing_provider_npi as {{ dbt.type_string() }}) as dispensing_provider_npi
    , cast(dispensing_date as date) as dispensing_date
    , cast(ndc_code as {{ dbt.type_string() }}) as ndc_code
    , cast(quantity as {{ dbt.type_int() }}) as quantity
    , cast(days_supply as {{ dbt.type_int() }}) as days_supply
    , cast(refills as {{ dbt.type_int() }}) as refills
    , cast(paid_date as date) as paid_date
    , cast(paid_amount as {{ dbt.type_numeric() }}) as paid_amount
    , cast(allowed_amount as {{ dbt.type_numeric() }}) as allowed_amount
    , cast(charge_amount as {{ dbt.type_numeric() }}) as charge_amount
    , cast(coinsurance_amount as {{ dbt.type_numeric() }}) as coinsurance_amount
    , cast(copayment_amount as {{ dbt.type_numeric() }}) as copayment_amount
    , cast(deductible_amount as {{ dbt.type_numeric() }}) as deductible_amount
    , cast(in_network_flag as {{ dbt.type_boolean() }}) as in_network_flag
    , cast(data_source as {{ dbt.type_string() }}) as data_source
    , cast(file_name as {{ dbt.type_string() }}) as file_name
    , cast(file_date as date) as file_date
    , cast(ingest_datetime as {{ dbt.type_timestamp() }}) as ingest_datetime
{% if var('use_synthetic_data', false) == true -%}
from {{ ref('tuva_data_assets', 'pharmacy_claim') }}
{%- else -%}
from {{ source('input', 'pharmacy_claim') }}
{%- endif %}