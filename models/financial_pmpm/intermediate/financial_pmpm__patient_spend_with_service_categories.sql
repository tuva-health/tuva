{{ config(
     enabled = var('financial_pmpm_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

with claims_with_service_categories as (
  select
      person_id
    , payer
    , {{ quote_column('plan') }}
    , service_category_1
    , service_category_2
    , coalesce(claim_start_date, claim_end_date) as claim_date
    , paid_amount
    , allowed_amount
    , data_source
  from {{ ref('financial_pmpm__stg_medical_claim') }}
)

, medical_claims_year_month as (
  select
      person_id
    , payer
    , {{ quote_column('plan') }}
    , service_category_1
    , service_category_2
    , {{ concat_custom([date_part('year', 'claim_date'),
                      dbt.right(
                      concat_custom(["'0'", date_part('month', 'claim_date')])
                      , 2)]) }} as year_month
    , paid_amount
    , allowed_amount
    , data_source
  from claims_with_service_categories
)

, rx_claims as (
  select
      person_id
    , payer
    , {{ quote_column('plan') }}
    , 'pharmacy' as service_category_1
    , cast(null as {{ dbt.type_string() }}) as service_category_2
    , coalesce(dispensing_date, paid_date) as claim_date
    , paid_amount
    , allowed_amount
    , data_source
  from {{ ref('financial_pmpm__stg_pharmacy_claim') }}
)

, rx_claims_year_month as (
  select
      person_id
    , payer
    , {{ quote_column('plan') }}
    , service_category_1
    , service_category_2
    , {{ concat_custom([date_part('year', 'claim_date'),
                      dbt.right(
                      concat_custom(["'0'", date_part('month', 'claim_date')])
                      , 2)]) }} as year_month
    , paid_amount
    , allowed_amount
    , data_source
  from rx_claims
)

, combine_medical_and_rx as (
select *
from medical_claims_year_month

union all

select *
from rx_claims_year_month
)

select
    person_id
  , year_month
  , payer
  , {{ quote_column('plan') }}
  , service_category_1
  , service_category_2
  , sum(paid_amount) as total_paid
  , sum(allowed_amount) as total_allowed
  , data_source
  , '{{ var('tuva_last_run') }}' as tuva_last_run
  from combine_medical_and_rx
group by
    person_id
  , year_month
  , payer
  , {{ quote_column('plan') }}
  , service_category_1
  , service_category_2
  , data_source
