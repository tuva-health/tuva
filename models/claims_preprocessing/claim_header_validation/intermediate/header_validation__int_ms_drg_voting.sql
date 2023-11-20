with normalize as(
    select 
        med.claim_id
        , med.data_source
        , ms.ms_drg_code
    from {{ ref('medical_claim') }} med
    inner join {{ ref('terminology__ms_drg') }} ms
        on med.bill_type_code = ms.ms_drg_code
    where claim_type = 'institutional'
)
, distinct_counts as(
    select 
        claim_id
        , data_source
        , ms_drg_code
        , count(*) as ms_drg_occurrence_count
    from normalize
    where ms_drg_code is not null
    group by 
        claim_id
        , data_source
        , ms_drg_code
)

, occurence_comparison as(
    select
        claim_id
        , data_source
        , ms_drg_code
        , ms_drg_occurrence_count
        , coalesce(lead(ms_drg_occurrence_count) 
            over (partition by claim_id, data_source order by ms_drg_occurrence_count desc),0) as next_occurrence_count
        , row_number() over (partition by claim_id, data_source order by ms_drg_occurrence_count desc) as occurrence_row_count
    from distinct_counts dist
)

select * from occurence_comparison
