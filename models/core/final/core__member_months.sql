{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False)) | as_bool
   )
}}

with month_start_and_end_dates as (
  select
      {% if target.type == 'fabric' %}
        concat(cast(year as {{ dbt.type_string() }} ),RIGHT(REPLICATE('0', 2) + cast(month as {{ dbt.type_string() }}), 2)) as year_month
      {% else %}
        concat(cast(year as {{ dbt.type_string() }} ),lpad(cast(month as {{ dbt.type_string() }}),2,'0')) as year_month
      {% endif %}
    , min(full_date) as month_start_date
    , max(full_date) as month_end_date
  from {{ ref('reference_data__calendar')}}
  group by year, month, year_month
)

select distinct
    a.patient_id
  , year_month
  , a.payer
  {% if target.type == 'fabric' %}
    , a."plan"
  {% else %}
    , a.plan
  {% endif %}
  , data_source
  , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('core__eligibility') }} a
inner join month_start_and_end_dates b
  on a.enrollment_start_date <= b.month_end_date
  and a.enrollment_end_date >= b.month_start_date

