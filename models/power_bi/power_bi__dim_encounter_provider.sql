with claim_provider_data as (
    select
        c.encounter_id
        , c.billing_id
        , c.paid_amount
        , case
            when p.npi is not null then 'INDIVIDUAL'
            when l.npi is not null then 'ENTITY'
            else 'unknown'
        end as provider_type
        , case
            when p.npi is not null then p.specialty
            when l.npi is not null then l.facility_type
            else null
        end as specialty
    from {{ ref('core__medical_claim') }} as c
    left join {{ ref('core__practitioner') }} as p
        on c.billing_id = p.npi
    left join {{ ref('core__location') }} as l
        on c.billing_id = l.npi
    where c.billing_id is not null
      and c.paid_amount is not null
)

, ranked_providers as (
    select
        encounter_id
        , billing_id
        , paid_amount
        , provider_type
        , specialty
        , row_number() over(
            partition by encounter_id
            order by paid_amount desc
        ) as overall_rank
        , row_number() over(
            partition by
                encounter_id
                , case when provider_type = 'INDIVIDUAL' then 1 else 0 end
            order by paid_amount desc
        ) as type_rank
        , max(case when provider_type = 'INDIVIDUAL' then 1 else 0 end)
            over(partition by encounter_id) as has_individual_flag
    from claim_provider_data
)

select
    encounter_id
    , billing_id as primary_provider_id
    , provider_type
    , specialty
    , paid_amount as paid_amount_decimal
from ranked_providers
where
    (has_individual_flag = 1 and provider_type = 'INDIVIDUAL' and type_rank = 1)
    or
    (has_individual_flag = 0 and overall_rank = 1)
order by encounter_id
