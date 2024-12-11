{{ config(
    enabled = var('claims_enabled', False)
) }}

with group_by_types_of_rb_claims as (

    select
          basic
        , hospice
        , loa
        , behavioral
        , count(*) as claim_count
        , count(*) * 100.0 / (
            select count(basic)
            from {{ ref('data_quality__rb_claims') }}
          ) as claim_percent
    from {{ ref('data_quality__rb_claims') }}
    group by basic, hospice, loa, behavioral

)

select
      basic
    , hospice
    , loa
    , behavioral
    , claim_count
    , claim_percent
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from group_by_types_of_rb_claims
