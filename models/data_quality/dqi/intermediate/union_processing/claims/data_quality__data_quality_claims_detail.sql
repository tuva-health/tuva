{{ config(
    enabled = (var('enable_legacy_data_quality', false) | as_bool) and 
              (var('claims_enabled', false) | as_bool)
    )
}}

{#- Athena/Trino exhausts query resources computing dense_rank() over the full
    union (300M+ rows) because of the global sort. On Athena, compute the rank
    over the small set of distinct dimension combinations and join it back
    (NULL-safe via `is not distinct from`). Other adapters keep the original
    single-pass window — same integer summary_sk, no behavior change. -#}
{% if target.type == 'athena' %}

with summary_sk_lookup as (
    select
        data_source
      , table_name
      , claim_type
      , field_name
      , dense_rank() over (
            order by data_source
                   , table_name
                   , claim_type
                   , field_name
        ) as summary_sk
    from (
        select distinct
            data_source
          , table_name
          , claim_type
          , field_name
        from {{ ref('data_quality__data_quality_claims_detail_union') }}
    )
)

select
    detail.data_source
  , detail.source_date
  , detail.table_name
  , detail.drill_down_key
  , detail.drill_down_value
  , detail.claim_type
  , detail.field_name
  , detail.bucket_name
  , detail.invalid_reason
  , detail.field_value
  , detail.tuva_last_run
  , sk.summary_sk
from {{ ref('data_quality__data_quality_claims_detail_union') }} as detail
inner join summary_sk_lookup as sk
    on detail.data_source is not distinct from sk.data_source
    and detail.table_name is not distinct from sk.table_name
    and detail.claim_type is not distinct from sk.claim_type
    and detail.field_name is not distinct from sk.field_name

{% else %}

select
    data_source
  , source_date
  , table_name
  , drill_down_key
  , drill_down_value
  , claim_type
  , field_name
  , bucket_name
  , invalid_reason
  , field_value
  , tuva_last_run
  , dense_rank() over (
        order by data_source
               , table_name
               , claim_type
               , field_name
    ) as summary_sk
from {{ ref('data_quality__data_quality_claims_detail_union') }}

{% endif %}
