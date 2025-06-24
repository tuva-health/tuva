{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
 | as_bool,
     tags = ['dqi', 'tuva_dqi_sev_2']
   )
}}

with eligibility as (
    select
        person_id
        , member_id
        , enrollment_start_date
        , enrollment_end_date
        , payer
        , {{ quote_column('plan') }}
        , data_source
    from {{ ref('input_layer__eligibility') }}
)

, pharmacy_claims as (
    select
        person_id
        , member_id
        , payer
        , {{ quote_column('plan') }}
        , data_source
        , dispensing_date
    from {{ ref('input_layer__pharmacy_claim') }}
)

, final as (
    select
        p.data_source
        , 'overlap' as test
        , count(*) as n_rows
    from pharmacy_claims as p
    inner join eligibility as e
    on p.person_id = e.person_id
    and p.member_id = e.member_id
    and p.payer = e.payer
    and p.{{ quote_column('plan') }} = e.{{ quote_column('plan') }}
    and p.dispensing_date between e.enrollment_start_date and e.enrollment_end_date
    group by p.data_source
    union all
    select
        data_source
        , 'pharmacy claim' as test
        , count(*) as n_rows
    from pharmacy_claims as pc
    group by pc.data_source
    union all
    select
        data_source
        , 'eligibility' as test
        , count(*) as n_rows
    from eligibility as el
    group by el.data_source
)

select
    data_source
    , test
    , n_rows
from final
where n_rows < 1
