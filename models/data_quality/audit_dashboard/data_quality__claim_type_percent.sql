{{ config(
    enabled = var('claims_enabled', var('tuva_marts_enabled', false)) | as_bool
) }}


with stage_cte as
(
    select
        sum(paid_amount) as paid_amount
        , count(distinct claim_id) as claim_type_count
        ,claim_type
        from {{ ref('medical_claim') }}
    group by claim_type

union all

    select
        sum(paid_amount) as paid_amount
        , count(distinct claim_id) as claim_type_count
        ,'pharmacy' as claim_type
    from {{ ref('pharmacy_claim') }}
)

,total_cte as (
    select
        sum(paid_amount) as total_paid_amount
        , sum(claim_type_count) as total_claim_count
    from stage_cte
)
, final_stage as(
    select
        claim_type
        , paid_amount/total_paid_amount as percent_of_total_paid
        , case
            when claim_type = 'pharmacy' then 1
            else claim_type_count/total_claim_count
    end as percent_of_total_distinct_claims
    , '{{ var('tuva_last_run') }}' as tuva_last_run
    from stage_cte
    cross join total_cte
)

select
    'institutional percent of total paid_amount' as data_quality_check
    , percent_of_total_paid as result_count
from final_stage
where claim_type = 'institutional'

union all

select
    'professional percent of total paid_amount' as data_quality_check
    , percent_of_total_paid as result_count
from final_stage
where claim_type = 'professional'

union all

select
    'pharmacy percent of total paid_amount' as data_quality_check
    , percent_of_total_paid as result_count
from final_stage
where claim_type = 'pharmacy'

union all

select
    'institutional percent of claim_type' as data_quality_check
    , percent_of_total_distinct_claims as result_count
from final_stage
where claim_type = 'institutional'

union all

select
    'professional percent of claim_type' as data_quality_check
    , percent_of_total_distinct_claims as result_count
from final_stage
where claim_type = 'professional'

union all

select
    'pharmacy percent of claim_type' as data_quality_check
    , percent_of_total_distinct_claims as result_count
from final_stage
where claim_type = 'pharmacy'