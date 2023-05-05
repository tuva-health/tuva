{{ config(
     enabled = var('pmpm_enabled',var('tuva_marts_enabled',True))
   )
}}

with medical as
(
    select
        patient_id
       ,cast({{ date_part("year", "claim_end_date" ) }} as {{ dbt.type_string() }} ) as year
       ,lpad(cast({{ date_part("month", "claim_end_date" ) }} as {{ dbt.type_string() }} ),2,'0') as month
       ,cast({{ date_part("year", "claim_end_date" ) }} as {{ dbt.type_string() }} )
            || lpad(cast({{ date_part("month", "claim_end_date" ) }} as {{ dbt.type_string() }} ),2,'0') AS year_month
       ,claim_type
       ,paid_amount
    from {{ ref('core__medical_claim') }}
)
, pharmacy as
(
    select
        patient_id
       ,cast({{ date_part("year", "dispensing_date" ) }} as {{ dbt.type_string() }} ) as year
       ,lpad(cast({{ date_part("month", "dispensing_date" ) }} as {{ dbt.type_string() }} ),2,'0') as month
       ,cast({{ date_part("year", "dispensing_date" ) }} as {{ dbt.type_string() }} )
            || lpad(cast({{ date_part("month", "dispensing_date" ) }} as {{ dbt.type_string() }} ),2,'0') AS year_month
       ,cast('pharmacy' as {{ dbt.type_string() }}) as claim_type
       ,paid_amount
    from {{ ref('core__pharmacy_claim') }}
)


select
    patient_id
    ,claim_type
    ,year_month
    ,count(*) as count_claims
    ,sum(paid_amount) as paid
from medical
group by
    patient_id
    ,claim_type
    ,year_month

union all

select
    patient_id
    ,claim_type
    ,year_month
    ,count(*) as count_claims
    ,sum(paid_amount) as paid
from pharmacy
group by
    patient_id
    ,claim_type
    ,year_month