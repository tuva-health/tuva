{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

with normalize as(
    select 
        med.claim_id
        , med.data_source
        , apr.apr_drg_code
    from {{ ref('normalized_input__stg_medical_claim') }} med
    inner join {{ ref('terminology__apr_drg') }} apr
        on med.apr_drg_code = apr.apr_drg_code
    where claim_type = 'institutional'
)
, distinct_counts as(
    select 
        claim_id
        , data_source
        , apr_drg_code
        , count(*) as apr_drg_occurrence_count
    from normalize
    where apr_drg_code is not null
    group by 
        claim_id
        , data_source
        , apr_drg_code
)

, occurence_comparison as(
    select
        claim_id
        , data_source
        , 'apr_drg_code' as column_name
        , apr_drg_code as normalized_code
        , apr_drg_occurrence_count as occurrence_count
        , coalesce(lead(apr_drg_occurrence_count) 
            over (partition by claim_id, data_source order by apr_drg_occurrence_count desc),0) as next_occurrence_count
        , row_number() over (partition by claim_id, data_source order by apr_drg_occurrence_count desc) as occurrence_row_count
    from distinct_counts dist
)

select
    claim_id
    , data_source
    , column_name
    , normalized_code
    , occurrence_count
    , next_occurrence_count
    , occurrence_row_count
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from occurence_comparison
