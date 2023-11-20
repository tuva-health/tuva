with normalize as(
    select 
        med.claim_id
        , med.data_source
        , admit.admit_type_code
    from {{ ref('medical_claim') }} med
    inner join terminology.admit_type admit
        on med.admit_type_code = admit.admit_type_code
    where claim_type = 'institutional'
)
, distinct_counts as(
    select 
        claim_id
        , data_source
        , admit_type_code
        , count(*) as admit_type_occurrence_count
    from normalize
    where admit_type_code is not null
    group by 
        claim_id
        , data_source
        , admit_type_code
)

, occurence_comparison as(
    select
        claim_id
        , data_source
        , admit_type_code
        , admit_source_occurrence_count
        , coalesce(lead(admit_type_occurrence_count) 
            over (partition by claim_id, data_source order by admit_type_occurrence_count desc),0) as next_occurrence_count
        , row_number() over (partition by claim_id, data_source order by admit_type_occurrence_count desc) as occurrence_row_count
    from distinct_counts dist
)

select * from occurence_comparison
