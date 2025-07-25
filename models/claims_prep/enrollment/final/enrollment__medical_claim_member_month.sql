with enrollment__stg_medical_claim as (
    select *
    from {{ ref('enrollment__stg_medical_claim') }}
),
enrollment__member_month as (
    select *
    from {{ ref('enrollment__member_month') }}
),
enrollment__patient as (
    select *
    from {{ ref('enrollment__patient') }}
)
select
    claim.medical_claim_sk
    , mm.member_month_sk
    , pat.patient_sk
from enrollment__stg_medical_claim as claim
    inner join enrollment__member_month as mm
    on claim.data_source = mm.data_source
    and claim.member_id = mm.member_id
    and claim.payer = mm.payer
    and claim.{{ quote_column('plan') }} = mm.{{ quote_column('plan') }}
    and claim.inferred_claim_start_date between mm.month_start_date and mm.month_end_date
    inner join enrollment__patient as pat
    on claim.member_id = pat.member_id
    and claim.data_source = pat.data_source
