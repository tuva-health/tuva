with normalize as(
    select 
        med.claim_id
        , med.data_source
        , apr.apr_drg_code
    from {{ ref('medical_claim') }} med
    inner join terminology.apr_drg apr
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
        , apr_drg_code
        , apr_drg_occurrence_count
        , coalesce(lead(apr_drg_occurrence_count) 
            over (partition by claim_id, data_source order by apr_drg_occurrence_count desc),0) as next_occurrence_count
        , row_number() over (partition by claim_id, data_source order by apr_drg_occurrence_count desc) as occurrence_row_count
    from distinct_counts dist
)

select * from occurence_comparison
