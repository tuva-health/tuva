{{ config(
     enabled = (var('enable_data_quality', false) | as_bool) and (var('claims_enabled', false) | as_bool),
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'pharmacy_claim_line_flags',
     tags = ['data_quality', 'dq', 'dq1', 'dq_logical'],
     materialized = 'table'
   )
}}

{% set string_type = dbt.type_string() %}

with source_rows as (
    select *
    from {{ ref('input_layer__pharmacy_claim') }}
),

final as (
    select
          source_rows.claim_id
        , source_rows.claim_line_number
        , source_rows.data_source
        , {{ dq_logical_int_flag_sql("source_rows.person_id is null") }} as person_id_null
        , {{ dq_logical_int_flag_sql("source_rows.dispensing_date is null") }} as dispensing_date_null
        , {{ dq_logical_int_flag_sql("source_rows.paid_date is null") }} as paid_date_null
        , {{ dq_logical_int_flag_sql("source_rows.prescribing_provider_npi is null") }} as prescribing_provider_npi_null
        , {{ dq_logical_int_flag_sql("source_rows.prescribing_provider_npi is not null and prescribing_provider_lookup.npi is null") }} as prescribing_provider_npi_invalid
        , {{ dq_logical_int_flag_sql("source_rows.dispensing_provider_npi is null") }} as dispensing_provider_npi_null
        , {{ dq_logical_int_flag_sql("source_rows.dispensing_provider_npi is not null and dispensing_provider_lookup.npi is null") }} as dispensing_provider_npi_invalid
        , {{ dq_logical_int_flag_sql("source_rows.ndc_code is null") }} as ndc_code_null
        , {{ dq_logical_int_flag_sql("source_rows.ndc_code is not null and ndc_lookup.ndc is null") }} as ndc_code_invalid
        , {{ dq_logical_int_flag_sql("source_rows.paid_amount is null") }} as paid_amount_null
        , {{ dq_logical_int_flag_sql("source_rows.paid_amount is not null and source_rows.paid_amount < 0") }} as paid_amount_lt_zero
        , {{ dq_logical_int_flag_sql("source_rows.allowed_amount is null") }} as allowed_amount_null
        , {{ dq_logical_int_flag_sql("source_rows.allowed_amount is not null and source_rows.allowed_amount < 0") }} as allowed_amount_lt_zero
        , {{ dq_logical_int_flag_sql("source_rows.paid_amount is not null and source_rows.allowed_amount is not null and source_rows.paid_amount > source_rows.allowed_amount") }} as paid_amount_gt_allowed_amount
    from source_rows
    left join {{ ref('provider_data__provider') }} as prescribing_provider_lookup
        on cast(source_rows.prescribing_provider_npi as {{ string_type }}) = cast(prescribing_provider_lookup.npi as {{ string_type }})
    left join {{ ref('provider_data__provider') }} as dispensing_provider_lookup
        on cast(source_rows.dispensing_provider_npi as {{ string_type }}) = cast(dispensing_provider_lookup.npi as {{ string_type }})
    left join {{ ref('terminology__ndc') }} as ndc_lookup
        on cast(source_rows.ndc_code as {{ string_type }}) = cast(ndc_lookup.ndc as {{ string_type }})
)

select *
from final
