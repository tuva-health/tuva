{{ config(
    enabled = var('claims_enabled', var('tuva_marts_enabled', false)) | as_bool
) }}


with duplicate_claim_type as(
    select
        claim_id
        , count(distinct claim_type) as claim_type_count
    from {{ ref('medical_claim') }}
    group by claim_id
)

select
    'multiple claim_type per claim' as data_quality_check
    , count(distinct claim_id) as result_count
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from duplicate_claim_type
where claim_type_count > 1