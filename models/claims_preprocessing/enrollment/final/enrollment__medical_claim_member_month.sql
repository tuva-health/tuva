with enrollment__stg_medical_claim as (
    select *
    from {{ ref('enrollment__stg_medical_claim') }}
),
enrollment__member_month as (
    select *
    from {{ ref('enrollment__member_month') }}
)
select
     claim.medical_claim_sk
    , mm.member_month_sk
from enrollment__stg_medical_claim as claim
    inner join enrollment__member_month as mm
    on claim.data_source = mm.data_source
    and claim.member_id = mm.member_id
    and claim.payer = mm.payer
    and claim.{{ quote_column('plan') }} = mm.{{ quote_column('plan') }}
    and claim.inferred_claim_start_date between mm.month_start_date and mm.month_end_date
