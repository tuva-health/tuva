{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
 | as_bool
   )
}}



with normalize_cte as (
    select
        med.claim_id
        , med.data_source
        , admit.admit_source_code

        , admit.admit_source_description
    from {{ ref('normalized_input__stg_medical_claim') }} as med
    inner join {{ ref('terminology__admit_source') }} as admit
        on med.admit_source_code = admit.admit_source_code
    where claim_type = 'institutional'
)

, distinct_counts as (
    select
        claim_id
        , data_source
        , admit_source_code
        , admit_source_description
        , count(*) as admit_source_occurrence_count
    from normalize_cte
    where admit_source_code is not null
    group by
        claim_id
        , data_source
        , admit_source_code
        , admit_source_description
)

, occurence_comparison as (
    select
        claim_id
        , data_source
        , 'admit_source_code' as column_name
        , admit_source_code as normalized_code
        , admit_source_description as normalized_description
        , admit_source_occurrence_count as occurrence_count
        , coalesce(lead(admit_source_occurrence_count)
            over (partition by claim_id, data_source
order by admit_source_occurrence_count desc), 0) as next_occurrence_count
        , row_number() over (partition by claim_id, data_source
order by admit_source_occurrence_count desc) as occurrence_row_count
    from distinct_counts as dist
)

select
    claim_id
    , data_source
    , column_name
    , normalized_code
    , normalized_description
    , occurrence_count
    , next_occurrence_count
    , occurrence_row_count
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from occurence_comparison
