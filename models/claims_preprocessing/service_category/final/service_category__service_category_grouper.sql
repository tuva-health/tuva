{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

with service_category_1_mapping as(
    select distinct 
        a.claim_id
        , a.claim_line_number
        , a.claim_type
        , case
            when s.service_category_1 is null then 'Value not in seed table'
            else s.service_category_1
          end service_category_1
        , case
            when b.service_category_2 is null then 'Not Mapped'
            when s.service_category_2 is null then 'Value not in seed table'
            else s.service_category_2
          end service_category_2
        , case
            when s.service_category_3 is null then 'Value not in seed table'
            else s.service_category_3
          end service_category_3
        , s.priority
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('service_category__stg_medical_claim') }} a
    left join {{ ref('service_category__combined_professional') }} b
    on a.claim_id = b.claim_id
    and a.claim_line_number = b.claim_line_number
    left join {{ ref('claims_preprocessing__service_category_seed') }} s on b.service_category_2 = s.service_category_2
    and
    b.service_category_3 = s.service_category_3
    where a.claim_type = 'professional'

    union all

    select distinct 
        a.claim_id
        , a.claim_line_number
        , a.claim_type
        , case
            when s.service_category_1 is null then 'Value not in seed table'
            else s.service_category_1
          end service_category_1
        , case
            when b.service_category_2 is null then 'Not Mapped'
            when s.service_category_2 is null then 'Value not in seed table'
            else s.service_category_2
          end service_category_2
        , case
            when s.service_category_3 is null then 'Value not in seed table'
            else s.service_category_3
          end service_category_3
        , s.priority
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('service_category__stg_medical_claim') }} a
    left join {{ ref('service_category__combined_institutional') }} b
    on a.claim_id = b.claim_id
    left join {{ ref('claims_preprocessing__service_category_seed') }} s on b.service_category_2 = s.service_category_2
    and
    b.service_category_3 = s.service_category_3
    where a.claim_type = 'institutional'
)
, service_category_2_deduplication as(
    select 
        claim_id
        , claim_line_number
        , claim_type
        , service_category_1
        , service_category_2
        , row_number() over (partition by claim_id, claim_line_number order by priority) as duplicate_row_number
    from service_category_1_mapping
)

select
    claim_id
    , claim_line_number
    , claim_type
    , service_category_1
    , service_category_2
    , service_category_3
from service_category_2_deduplication
where duplicate_row_number = 1