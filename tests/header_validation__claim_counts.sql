

with source as(
    select
        data_source
        , count(distinct claim_id) as distinct_claim_count
    from {{ ref('medical_claim') }}
    group by
        data_source
)
, header_validation as(
    select
        data_source
        , count(distinct claim_id) as distinct_claim_count
    from {{ ref('normalized_input__medical_claim') }}
    group by
        data_source

)

select * from source s
inner join header_validation h
    on s.data_source = h.data_source
where s.distinct_claim_count <> h.distinct_claim_count
