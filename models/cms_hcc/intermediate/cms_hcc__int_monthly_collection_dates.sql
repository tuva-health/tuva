{{ config(
     enabled = var('cms_hcc_enabled',var('claims_enabled',var('tuva_marts_enabled',false))) | as_bool
   )
}}

with collection_date_range as (

    select
          cast(min({{ date_trunc("year", "claim_end_date") }}) as date) as start_date
        , cast(max(claim_end_date) as date) as end_date
    from {{ ref('cms_hcc__stg_core__medical_claim') }}

)

select distinct
      cast({{ date_trunc("year", "calendar.last_day_of_month") }} as date) as collection_start_date
    , cast(calendar.last_day_of_month as date) as collection_end_date
    , cast(calendar.year as integer) as collection_year
    , cast(calendar.year + 1 as integer) as payment_year
from {{ ref('reference_data__calendar') }} as calendar
inner join collection_date_range
    on calendar.first_day_of_month
        between collection_date_range.start_date and collection_date_range.end_date
