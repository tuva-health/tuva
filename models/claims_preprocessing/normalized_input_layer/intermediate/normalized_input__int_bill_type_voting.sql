with normalize as(
    select 
        med.claim_id
        , med.data_source
        , bill.bill_type_code
    from {{ ref('normalized_input__stg_medical_claim') }} med
    inner join {{ ref('terminology__bill_type') }} bill
        on med.bill_type_code = bill.bill_type_code
    where claim_type = 'institutional'
)
, distinct_counts as(
    select 
        claim_id
        , data_source
        , bill_type_code
        , count(*) as bill_type_occurrence_count
    from normalize
    where bill_type_code is not null
    group by 
        claim_id
        , data_source
        , bill_type_code
)

, occurence_comparison as(
    select
        claim_id
        , data_source
        , 'bill_type_code' as column_name
        , bill_type_code as normalized_code
        , bill_type_occurrence_count as occurrence_count
        , coalesce(lead(bill_type_occurrence_count) 
            over (partition by claim_id, data_source order by bill_type_occurrence_count desc),0) as next_occurrence_count
        , row_number() over (partition by claim_id, data_source order by bill_type_occurrence_count desc) as occurrence_row_count
    from distinct_counts dist
)

select * from occurence_comparison
