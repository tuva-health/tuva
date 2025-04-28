{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
 | as_bool
   )
}}


with stg_eligibility as (
    select
      cast(elig.person_id as {{ dbt.type_string() }}) as person_id
      , cast(elig.member_id as {{ dbt.type_string() }}) as member_id
      , cast(elig.subscriber_id as {{ dbt.type_string() }}) as subscriber_id
      , cast(elig.gender as {{ dbt.type_string() }}) as gender
      , cast(elig.race as {{ dbt.type_string() }}) as race
      , cast(date_norm.normalized_birth_date as date) as birth_date
      , cast(date_norm.normalized_death_date as date) as death_date
      , cast(elig.death_flag as int) as death_flag
      , cast(date_norm.normalized_enrollment_start_date as date) as enrollment_start_date
      , cast(date_norm.normalized_enrollment_end_date as date) as enrollment_end_date
      , cast(elig.payer as {{ dbt.type_string() }}) as payer
      , cast(elig.payer_type as {{ dbt.type_string() }}) as payer_type
      , cast(elig.{{ quote_column('plan') }} as {{ dbt.type_string() }}) as {{ quote_column('plan') }}
      , cast(elig.original_reason_entitlement_code as {{ dbt.type_string() }}) as original_reason_entitlement_code
      , cast(elig.dual_status_code as {{ dbt.type_string() }}) as dual_status_code
      , cast(elig.medicare_status_code as {{ dbt.type_string() }}) as medicare_status_code
      , cast(elig.group_id as {{ dbt.type_string() }}) as group_id
      , cast(elig.group_name as {{ dbt.type_string() }}) as group_name
      , cast(elig.first_name as {{ dbt.type_string() }}) as first_name
      , cast(elig.last_name as {{ dbt.type_string() }}) as last_name
      , cast(elig.social_security_number as {{ dbt.type_string() }}) as social_security_number
      , cast(elig.subscriber_relation as {{ dbt.type_string() }}) as subscriber_relation
      , cast(elig.address as {{ dbt.type_string() }}) as address
      , cast(elig.city as {{ dbt.type_string() }}) as city
      , cast(elig.state as {{ dbt.type_string() }}) as state
      , cast(elig.zip_code as {{ dbt.type_string() }}) as zip_code
      , cast(elig.phone as {{ dbt.type_string() }}) as phone
      , cast(elig.data_source as {{ dbt.type_string() }}) as data_source
      , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_string() }}) as tuva_last_run
    from {{ ref('normalized_input__stg_eligibility') }} as elig
    left outer join {{ ref('normalized_input__int_eligibility_dates_normalize') }} as date_norm
      on elig.person_id_key = date_norm.person_id_key
)
, month_start_and_end_dates as (
  select
    {{ concat_custom(["year",
                  dbt.right(concat_custom(["'0'", "month"]), 2)]) }} as year_month
    , min(full_date) as month_start_date
    , max(full_date) as month_end_date
  from {{ ref('reference_data__calendar') }}
  group by year, month, year_month
)
, member_month_calc as (
  select distinct
    dense_rank() over (
      order by
        a.person_id
      , b.year_month
      , a.payer
      , a.{{ quote_column('plan') }}
      , a.data_source
      ) as member_month_key
    , a.person_id
    , b.year_month
    , a.payer
    , a.{{ quote_column('plan') }}
    , a.data_source
  from stg_eligibility as a
  inner join month_start_and_end_dates as b
    on a.enrollment_start_date <= b.month_end_date
    and a.enrollment_end_date >= b.month_start_date
)
select 
    m.member_month_key
  , a.person_id
  , a.member_id
  , a.subscriber_id
  , a.gender
  , a.race
  , a.birth_date
  , a.death_date
  , a.death_flag
  , a.enrollment_start_date
  , a.enrollment_end_date
  , a.payer
  , a.payer_type
  , a.{{ quote_column('plan') }}
  , b.year_month
  , a.original_reason_entitlement_code
  , a.dual_status_code
  , a.medicare_status_code
  , a.group_id
  , a.group_name
  , a.first_name
  , a.last_name
  , a.social_security_number
  , a.subscriber_relation
  , a.address
  , a.city
  , a.state
  , a.zip_code
  , a.phone
  , a.data_source
  , a.tuva_last_run
from stg_eligibility as a
  inner join month_start_and_end_dates as b
    on a.enrollment_start_date <= b.month_end_date
    and a.enrollment_end_date >= b.month_start_date
  inner join member_month_calc as m
    on m.person_id = a.person_id
    and m.payer = a.payer
    and m.{{ quote_column('plan') }} = a.{{ quote_column('plan') }}
    and m.data_source = a.data_source
    and m.year_month = b.year_month