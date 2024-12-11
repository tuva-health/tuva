{{ config(
    enabled = var('claims_enabled', False)
) }}

with rb as (

    select distinct 
        claim_id 
    from {{ ref('data_quality__rb_claims') }}
    -- Here it is important to use Tuva Logic
    -- to only get R&B claims with basic = 1:
    where basic = 1
    -- where has_a_valid_rev_code = 1

)

, drg as (

    select distinct 
        claim_id 
    from {{ ref('data_quality__drg') }}

)

, bill as (

    select distinct 
        claim_id 
    from {{ ref('data_quality__bill') }}

)

, rb_drg as (

    select distinct 
        rb.claim_id 
    from rb 
    inner join drg 
        on rb.claim_id = drg.claim_id

)

, rb_bill as (

    select distinct 
        rb.claim_id 
    from rb 
    inner join bill 
        on rb.claim_id = bill.claim_id

)

, drg_bill as (

    select distinct 
        drg.claim_id 
    from drg 
    inner join bill 
        on drg.claim_id = bill.claim_id

)

, rb_drg_bill as (

    select distinct 
        rb.claim_id 
    from rb 
    inner join drg 
        on rb.claim_id = drg.claim_id
    inner join bill 
        on rb.claim_id = bill.claim_id

)

, all_claims_in_all_above_cohorts as (

    select * from rb 
    union all
    select * from drg 
    union all
    select * from bill 
    union all
    select * from rb_drg 
    union all
    select * from rb_bill 
    union all
    select * from drg_bill 
    union all
    select * from rb_drg_bill

)

, all_distinct_claims as (

    select distinct 
        claim_id 
    from all_claims_in_all_above_cohorts

)

select 
      aa.claim_id
    , case when rb.claim_id is not null then 1 else 0 end as rb
    , case when drg.claim_id is not null then 1 else 0 end as drg
    , case when bill.claim_id is not null then 1 else 0 end as bill
    , case when rb_drg.claim_id is not null then 1 else 0 end as rb_drg
    , case when rb_bill.claim_id is not null then 1 else 0 end as rb_bill
    , case when drg_bill.claim_id is not null then 1 else 0 end as drg_bill
    , case when rb_drg_bill.claim_id is not null then 1 else 0 end as rb_drg_bill
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from all_distinct_claims aa
left join rb on aa.claim_id = rb.claim_id
left join drg on aa.claim_id = drg.claim_id
left join bill on aa.claim_id = bill.claim_id
left join rb_drg on aa.claim_id = rb_drg.claim_id
left join rb_bill on aa.claim_id = rb_bill.claim_id
left join drg_bill on aa.claim_id = drg_bill.claim_id
left join rb_drg_bill on aa.claim_id = rb_drg_bill.claim_id
