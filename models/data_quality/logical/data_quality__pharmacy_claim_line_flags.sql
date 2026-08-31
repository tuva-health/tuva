{{ config(
     enabled = (the_tuva_project.tuva_boolean_var('data_quality_enabled', false)) and (the_tuva_project.tuva_boolean_var('claims_enabled', false)),
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'pharmacy_claim_line_flags',
     tags = ['data_quality', 'dq_logical'],
     materialized = 'table'
   )
}}

{% set string_type = dbt.type_string() %}
{% set current_date_sql = dq_current_date_sql() %}
{% set min_recent_claim_date_sql = dq_date_literal_sql('2000-01-01') %}
{% set legacy_open_end_date_sql = dq_date_literal_sql('9999-12-31') %}
{% set tuva_last_run_timestamp_sql =
    "cast('" ~ var('tuva_last_run') ~ "' as " ~ dbt.type_timestamp() ~ ")"
%}
{% set eligibility_match_applicable_where_sql =
    "source_rows.person_id is not null"
    ~ " and source_rows.member_id is not null"
    ~ " and source_rows.payer is not null"
    ~ " and source_rows." ~ quote_column('plan') ~ " is not null"
    ~ " and source_rows.data_source is not null"
    ~ " and source_rows.paid_date is not null"
    ~ " and " ~ dq_date_in_range_where_sql('source_rows.paid_date', min_recent_claim_date_sql, current_date_sql)
    ~ " and " ~ dq_member_month_spine_date_where_sql('source_rows.paid_date')
    ~ " and source_rows.paid_date <= cast(" ~ tuva_last_run_timestamp_sql ~ " as date)"
%}
with eligibility_rows_with_effective_end_date as (
    select
          eligibility_rows.*
        , case
            when eligibility_rows.enrollment_end_date is null
              or eligibility_rows.enrollment_end_date = {{ legacy_open_end_date_sql }}
              or eligibility_rows.enrollment_end_date > cast({{ tuva_last_run_timestamp_sql }} as date)
                then cast({{ tuva_last_run_timestamp_sql }} as date)
            else eligibility_rows.enrollment_end_date
          end as _dq_effective_enrollment_end_date
    from {{ ref('input_layer__eligibility') }} as eligibility_rows
),

source_rows as (
    select *
    from {{ ref('input_layer__pharmacy_claim') }}
),

source_eligibility_member_months as (
    select distinct
          source_rows.person_id
        , source_rows.member_id
        , source_rows.payer
        , source_rows.{{ quote_column('plan') }}
        , source_rows.data_source
        , (
              {{ date_part('year', 'source_rows.paid_date') }} * 100
              + {{ date_part('month', 'source_rows.paid_date') }}
          ) as _dq_claim_year_month
    from source_rows
    where {{ eligibility_match_applicable_where_sql }}
),

matching_eligibility_member_months as (
    select distinct
          source_member_months.person_id
        , source_member_months.member_id
        , source_member_months.payer
        , source_member_months.{{ quote_column('plan') }}
        , source_member_months.data_source
        , source_member_months._dq_claim_year_month
        , 1 as _dq_has_matching_eligibility
    from source_eligibility_member_months as source_member_months
    inner join eligibility_rows_with_effective_end_date as eligibility_rows
        on eligibility_rows.person_id = source_member_months.person_id
       and eligibility_rows.member_id = source_member_months.member_id
       and eligibility_rows.payer = source_member_months.payer
       and eligibility_rows.{{ quote_column('plan') }} = source_member_months.{{ quote_column('plan') }}
       and eligibility_rows.data_source = source_member_months.data_source
       and source_member_months._dq_claim_year_month >= (
            {{ date_part('year', 'eligibility_rows.enrollment_start_date') }} * 100
            + {{ date_part('month', 'eligibility_rows.enrollment_start_date') }}
       )
       and source_member_months._dq_claim_year_month <= (
            {{ date_part('year', 'eligibility_rows._dq_effective_enrollment_end_date') }} * 100
            + {{ date_part('month', 'eligibility_rows._dq_effective_enrollment_end_date') }}
       )
),

final as (
    select
          source_rows.claim_id
        , source_rows.claim_line_number
        , source_rows.data_source
        , {{ dq_logical_int_flag_sql(
            "source_rows.claim_line_number <= 0",
            "source_rows.claim_line_number is not null"
          ) }} as claim_line_number_not_positive
        , {{ dq_logical_int_flag_sql("source_rows.person_id is null", "1 = 1") }} as person_id_null
        , {{ dq_logical_binary_value_flag_sql("source_rows.in_network_flag") }} as in_network_flag_invalid
        , {{ dq_logical_int_flag_sql("source_rows.dispensing_date is null", "1 = 1") }} as dispensing_date_null
        , {{ dq_logical_int_flag_sql("source_rows.paid_date is null", "1 = 1") }} as paid_date_null
        , {{ dq_logical_int_flag_sql(
            "source_rows.dispensing_date < " ~ min_recent_claim_date_sql ~ " or source_rows.dispensing_date > " ~ current_date_sql,
            "source_rows.dispensing_date is not null"
          ) }} as dispensing_date_out_of_reasonable_range
        , {{ dq_logical_int_flag_sql(
            "source_rows.paid_date < " ~ min_recent_claim_date_sql ~ " or source_rows.paid_date > " ~ current_date_sql,
            "source_rows.paid_date is not null"
          ) }} as paid_date_out_of_reasonable_range
        , {{ dq_logical_date_range_flag_sql(
            "source_rows.file_date",
            min_recent_claim_date_sql,
            current_date_sql
          ) }} as file_date_outside_supported_date_range
        , {{ dq_logical_ingest_datetime_range_flag_sql(
            "source_rows.ingest_datetime"
          ) }} as ingest_datetime_out_of_reasonable_range
        , {{ dq_logical_int_flag_sql("source_rows.prescribing_provider_npi is null", "1 = 1") }} as prescribing_provider_npi_null
        , {{ dq_logical_int_flag_sql(
            "prescribing_provider_lookup.npi is null",
            "source_rows.prescribing_provider_npi is not null"
          ) }} as prescribing_provider_npi_invalid
        , {{ dq_logical_int_flag_sql("source_rows.dispensing_provider_npi is null", "1 = 1") }} as dispensing_provider_npi_null
        , {{ dq_logical_int_flag_sql(
            "dispensing_provider_lookup.npi is null",
            "source_rows.dispensing_provider_npi is not null"
          ) }} as dispensing_provider_npi_invalid
        , {{ dq_logical_int_flag_sql("source_rows.ndc_code is null", "1 = 1") }} as ndc_code_null
        , {{ dq_logical_int_flag_sql(
            "ndc_lookup.ndc11 is null",
            "source_rows.ndc_code is not null"
          ) }} as ndc_code_invalid
        , {{ dq_logical_int_flag_sql("source_rows.quantity is null", "1 = 1") }} as quantity_null
        , {{ dq_logical_int_flag_sql(
            "source_rows.quantity <= 0",
            "source_rows.quantity is not null"
          ) }} as quantity_not_positive
        , {{ dq_logical_int_flag_sql("source_rows.days_supply is null", "1 = 1") }} as days_supply_null
        , {{ dq_logical_int_flag_sql(
            "source_rows.days_supply <= 0",
            "source_rows.days_supply is not null"
          ) }} as days_supply_not_positive
        , {{ dq_logical_int_flag_sql(
            "source_rows.refills < 0",
            "source_rows.refills is not null"
          ) }} as refills_lt_zero
        , {{ dq_logical_int_flag_sql("source_rows.paid_amount is null", "1 = 1") }} as paid_amount_null
        , {{ dq_logical_int_flag_sql(
            "source_rows.paid_amount < 0",
            "source_rows.paid_amount is not null"
          ) }} as paid_amount_lt_zero
        , {{ dq_logical_int_flag_sql("source_rows.allowed_amount is null", "1 = 1") }} as allowed_amount_null
        , {{ dq_logical_int_flag_sql(
            "source_rows.allowed_amount < 0",
            "source_rows.allowed_amount is not null"
          ) }} as allowed_amount_lt_zero
        , {{ dq_logical_int_flag_sql(
            "source_rows.paid_amount > source_rows.allowed_amount",
            "source_rows.paid_amount is not null and source_rows.allowed_amount is not null"
          ) }} as paid_amount_gt_allowed_amount
        , {{ dq_logical_int_flag_sql(
            "matching_eligibility_member_months._dq_has_matching_eligibility is null",
            eligibility_match_applicable_where_sql
          ) }} as no_matching_eligibility_span
    from source_rows
    left join matching_eligibility_member_months
        on matching_eligibility_member_months.person_id = source_rows.person_id
       and matching_eligibility_member_months.member_id = source_rows.member_id
       and matching_eligibility_member_months.payer = source_rows.payer
       and matching_eligibility_member_months.{{ quote_column('plan') }} = source_rows.{{ quote_column('plan') }}
       and matching_eligibility_member_months.data_source = source_rows.data_source
       and matching_eligibility_member_months._dq_claim_year_month = (
            {{ date_part('year', 'source_rows.paid_date') }} * 100
            + {{ date_part('month', 'source_rows.paid_date') }}
       )
    left join {{ ref('provider_data__provider') }} as prescribing_provider_lookup
        on cast(source_rows.prescribing_provider_npi as {{ string_type }}) = cast(prescribing_provider_lookup.npi as {{ string_type }})
    left join {{ ref('provider_data__provider') }} as dispensing_provider_lookup
        on cast(source_rows.dispensing_provider_npi as {{ string_type }}) = cast(dispensing_provider_lookup.npi as {{ string_type }})
    left join {{ ref('core__stg_coderx_packages') }} as ndc_lookup
        on cast(source_rows.ndc_code as {{ string_type }})
           = cast(ndc_lookup.ndc11 as {{ string_type }})
)

select *
from final
