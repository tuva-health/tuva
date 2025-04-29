{% if var('enable_pharmacy_claim', false) == true -%}

select *
from {{ ref('pharmacy_claim') }}

{% elif var('enable_pharmacy_claim', false) == false -%}

{% if target.type == 'fabric' %}
select top 0
      cast(null as {{ dbt.type_string() }} ) as claim_id
    , cast(null as {{ dbt.type_int() }} ) as claim_line_number
    , cast(null as {{ dbt.type_string() }} ) as person_id
    , cast(null as {{ dbt.type_string() }} ) as member_id
    , cast(null as {{ dbt.type_string() }} ) as payer
    , cast(null as {{ dbt.type_string() }} ) as {{ quote_column('plan') }}
    , cast(null as {{ dbt.type_string() }} ) as prescribing_provider_npi
    , cast(null as {{ dbt.type_string() }} ) as dispensing_provider_npi
    , cast(null as {{ dbt.type_date() }} ) as dispensing_date
    , cast(null as {{ dbt.type_string() }} ) as ndc_code
    , cast(null as {{ dbt.type_int() }} ) as quantity
    , cast(null as {{ dbt.type_int() }} ) as days_supply
    , cast(null as {{ dbt.type_int() }} ) as refills
    , cast(null as {{ dbt.type_date() }} ) as paid_date
    , cast(null as {{ dbt.type_float() }} ) as paid_amount
    , cast(null as {{ dbt.type_float() }} ) as allowed_amount
    , cast(null as {{ dbt.type_float() }} ) as charge_amount
    , cast(null as {{ dbt.type_float() }} ) as coinsurance_amount
    , cast(null as {{ dbt.type_float() }} ) as copayment_amount
    , cast(null as {{ dbt.type_float() }} ) as deductible_amount
    , cast(null as {{ dbt.type_int() }} ) as in_network_flag
    , cast(null as {{ dbt.type_string() }} ) as data_source
    , cast(null as {{ dbt.type_string() }} ) as file_name
    , cast(null as {{ dbt.type_date() }} ) as file_date
    , cast(null as {{ dbt.type_timestamp() }} ) as ingest_datetime
    , cast(null as {{ dbt.type_timestamp() }} ) as tuva_last_run -- Added based on pattern
{% else %}
select
      cast(null as {{ dbt.type_string() }} ) as claim_id
    , cast(null as {{ dbt.type_int() }} ) as claim_line_number
    , cast(null as {{ dbt.type_string() }} ) as person_id
    , cast(null as {{ dbt.type_string() }} ) as member_id
    , cast(null as {{ dbt.type_string() }} ) as payer
    , cast(null as {{ dbt.type_string() }} ) as {{ quote_column('plan') }}
    , cast(null as {{ dbt.type_string() }} ) as prescribing_provider_npi
    , cast(null as {{ dbt.type_string() }} ) as dispensing_provider_npi
    , cast(null as {{ dbt.type_date() }} ) as dispensing_date
    , cast(null as {{ dbt.type_string() }} ) as ndc_code
    , cast(null as {{ dbt.type_int() }} ) as quantity
    , cast(null as {{ dbt.type_int() }} ) as days_supply
    , cast(null as {{ dbt.type_int() }} ) as refills
    , cast(null as {{ dbt.type_date() }} ) as paid_date
    , cast(null as {{ dbt.type_float() }} ) as paid_amount
    , cast(null as {{ dbt.type_float() }} ) as allowed_amount
    , cast(null as {{ dbt.type_float() }} ) as charge_amount
    , cast(null as {{ dbt.type_float() }} ) as coinsurance_amount
    , cast(null as {{ dbt.type_float() }} ) as copayment_amount
    , cast(null as {{ dbt.type_float() }} ) as deductible_amount
    , cast(null as {{ dbt.type_int() }} ) as in_network_flag
    , cast(null as {{ dbt.type_string() }} ) as data_source
    , cast(null as {{ dbt.type_string() }} ) as file_name
    , cast(null as {{ dbt.type_date() }} ) as file_date
    , cast(null as {{ dbt.type_timestamp() }} ) as ingest_datetime
    , cast(null as {{ dbt.type_timestamp() }} ) as tuva_last_run -- Added based on pattern
limit 0
{%- endif %}

{%- endif %}
