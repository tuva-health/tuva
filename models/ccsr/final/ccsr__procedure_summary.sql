with procedure_base as (
    select 
        encounter_id
        , claim_id
        , normalized_code
        , code_description
        , ccsr_parent_category
        , ccsr_category
        , ccsr_category_description,
        , clinical_domain
        , operation
        , approach
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('ccsr__long_procedure_category') }} c
    where ccsr_category is not null
),

procedures_aggregated as (
    select
        ccsr_category
        , ccsr_category_description
        , operation
        , approach
        , count(distinct claim_id) as n_occurrences
        , count(distinct person_id) as n_individuals
    from procedure_base
    group by 
        ccsr_category
        , ccsr_category_description
        , operation
        , approach
)

select * from procedures_aggregated
