{{ config(
     enabled = the_tuva_project.tuva_boolean_var('claims_enabled', false)
   )
}}

{%- set coderx_name_type = 'string' if target.type in ['bigquery', 'databricks'] else 'varchar(3000)' -%}

select
      cast(claim_id as {{ dbt.type_string() }}) as claim_id
    , cast(claim_line_number as int) as claim_line_number
    , cast(person_id as {{ dbt.type_string() }}) as person_id
    , cast(member_id as {{ dbt.type_string() }}) as member_id
    , cast(payer as {{ dbt.type_string() }}) as payer
    , cast({{ quote_column('plan') }} as {{ dbt.type_string() }}) as {{ quote_column('plan') }}
    , cast(prescribing_provider_npi as {{ dbt.type_string() }}) as prescribing_provider_id
    , case
      when pres.entity_type_code = '1' then cast(
        {{ concat_custom([
             "pres.provider_last_name"
           , "', '"
           , "pres.provider_first_name"
        ]) }} as {{ dbt.type_string() }})
      when pres.entity_type_code = '2' then cast(pres.provider_organization_name as {{ dbt.type_string() }})
      else null
    end as prescribing_provider_name
    , cast(dispensing_provider_npi as {{ dbt.type_string() }}) as dispensing_provider_id
    , case
      when disp.entity_type_code = '1' then cast(
        {{ concat_custom([
             "disp.provider_last_name"
           , "', '"
           , "disp.provider_first_name"
        ]) }} as {{ dbt.type_string() }})
      when disp.entity_type_code = '2' then cast(disp.provider_organization_name as {{ dbt.type_string() }})
      else null
    end as dispensing_provider_name
    , cast(dispensing_date as date) as dispensing_date
    , cast(ndc_code as {{ dbt.type_string() }}) as ndc_code
    , cast(coderx_packages.drug_name as {{ coderx_name_type }}) as ndc_description
    , cast(quantity as int) as quantity
    , cast(days_supply as int) as days_supply
    , cast(refills as int) as refills
    , cast(paid_date as date) as paid_date
    , cast(paid_amount as {{ dbt.type_numeric() }}) as paid_amount
    , cast(allowed_amount as {{ dbt.type_numeric() }}) as allowed_amount
    , cast(charge_amount as {{ dbt.type_numeric() }}) as charge_amount
    , cast(coinsurance_amount as {{ dbt.type_numeric() }}) as coinsurance_amount
    , cast(copayment_amount as {{ dbt.type_numeric() }}) as copayment_amount
    , cast(deductible_amount as {{ dbt.type_numeric() }}) as deductible_amount
    , cast(in_network_flag as {{ dbt.type_int() }}) as in_network_flag
    , cast(data_source as {{ dbt.type_string() }}) as data_source
    , cast(pharm.file_date as date) as file_date
    , cast(pharm.ingest_datetime as {{ dbt.type_timestamp() }}) as ingest_datetime
    , cast(pharm.file_name as {{ dbt.type_string() }}) as file_name
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
    {{ select_extension_columns(ref('input_layer__pharmacy_claim'), alias='pharm', strip_prefix=false) }}
from {{ ref('normalized_input__stg_pharmacy_claim') }} as pharm
left outer join {{ ref('provider_data__provider') }} as pres
      on pharm.prescribing_provider_npi = pres.npi
left outer join {{ ref('provider_data__provider') }} as disp
      on pharm.dispensing_provider_npi = disp.npi
left outer join {{ ref('core__stg_coderx_packages') }} as coderx_packages
      on cast(pharm.ndc_code as {{ dbt.type_string() }}) = coderx_packages.ndc11
