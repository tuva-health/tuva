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
),

medical_claims as (
    select
        person_id
        , member_id
        , payer
        , {{ quote_column('plan') }}
        , data_source
        , coalesce(claim_start_date, admission_date, claim_line_start_date) 
            as inferred_claim_start_date
    from {{ ref('input_layer__medical_claim') }}
),

final as (
    select  
        m.data_source
        , 'overlap' as test
        , count(*) as n_rows
    from medical_claims as m
    inner join eligibility as e
    on m.person_id = e.person_id
    and m.member_id = e.member_id
    and m.payer = e.payer
    and m.{{ quote_column('plan') }} = e.{{ quote_column('plan') }}
    and m.inferred_claim_start_date between e.enrollment_start_date and e.enrollment_end_date
    group by m.data_source
    union all
    select
        mc.data_source
        , 'medical claim' as test
        , count(*) as n_rows
    from medical_claims as mc
    group by mc.data_source
    union all
    select
        el.data_source
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
