{{ config(
     enabled = var('ccsr_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with procedure_base as (
    select
        encounter_id
        , claim_id
        , normalized_code
        , code_description
        , ccsr_parent_category
        , ccsr_category
        , ccsr_category_description
        , clinical_domain
        , operation
        , approach
        , count(claim_id) over
            (partition by ccsr_category, operation) as n_total_occurrences
    from {{ ref('ccsr__long_procedure_category') }}
    -- include only records that map to a CCSR procedure category
    where ccsr_category is not null
)

, procedures_aggregated as (
    select
        ccsr_category
        , ccsr_category_description
        , operation
        , approach
        , count(claim_id) as n_occurrences_with_approach
        , n_total_occurrences
        , count(claim_id) / n_total_occurrences * 100 as approach_rate
    from procedure_base
    group by
        ccsr_category
        , ccsr_category_description
        , operation
        , approach
        , n_total_occurrences
)

select
    *
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from procedures_aggregated
