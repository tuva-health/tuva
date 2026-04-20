{{ config(
     enabled = var('claims_enabled', false) | as_bool,
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'pharmacy_claim_claim_flags',
     tags = ['data_quality', 'dqi', 'dq1', 'dq_logical'],
     materialized = 'table'
   )
}}

{% set string_type = dbt.type_string() %}
{% set source_sentinel = dq_source_key_sentinel() %}

with source_rows as (
    select *
    from {{ ref('input_layer__pharmacy_claim') }}
),

aggregated_claims as (
    select
          source_rows.claim_id
        , source_rows.data_source
        , count(distinct case when source_rows.person_id is not null then source_rows.person_id end) as person_id_distinct_count
    from source_rows
    group by 1, 2
),

missing_eligibility_claims as (
    select distinct
          claim_rows.claim_id
        , claim_rows.data_source
    from source_rows as claim_rows
    where claim_rows.person_id is not null
      and (claim_rows.dispensing_date is not null or claim_rows.paid_date is not null)
      and not exists (
          select 1
          from {{ ref('input_layer__eligibility') }} as eligibility_rows
          where coalesce(cast(eligibility_rows.data_source as {{ string_type }}), '{{ source_sentinel }}') = coalesce(cast(claim_rows.data_source as {{ string_type }}), '{{ source_sentinel }}')
            and eligibility_rows.person_id = claim_rows.person_id
            and (
                (claim_rows.dispensing_date is not null and claim_rows.dispensing_date between eligibility_rows.enrollment_start_date and eligibility_rows.enrollment_end_date)
                or
                (claim_rows.paid_date is not null and claim_rows.paid_date between eligibility_rows.enrollment_start_date and eligibility_rows.enrollment_end_date)
            )
      )
),

final as (
    select
          aggregated_claims.claim_id
        , aggregated_claims.data_source
        , {{ dq_logical_int_flag_sql("aggregated_claims.person_id_distinct_count > 1") }} as multiple_person_ids_per_claim
        , {{ dq_logical_int_flag_sql("missing_eligibility_claims.claim_id is not null") }} as no_matching_eligibility_span
    from aggregated_claims
    left join missing_eligibility_claims
        on aggregated_claims.claim_id = missing_eligibility_claims.claim_id
       and (
            aggregated_claims.data_source = missing_eligibility_claims.data_source
            or (
                aggregated_claims.data_source is null
                and missing_eligibility_claims.data_source is null
            )
       )
)

select *
from final
