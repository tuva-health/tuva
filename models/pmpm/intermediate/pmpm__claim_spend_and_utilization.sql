{{ config(enabled = var('pmpm_enabled',var('tuva_packages_enabled',True)) ) }}


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
    from {{ var('medical_claim') }}
)
, pharmacy as

{# jinja to use an empty pharmacy_claim table if the pharmacy_claim_exists var is set to false, or the node in the pharmacy_claim variable otherwise  #}
{% if var('pharmacy_claim_exists',True) %}
(
    select
        patient_id
       ,cast({{ date_part("year", "dispensing_date" ) }} as {{ dbt.type_string() }} ) as year
       ,lpad(cast({{ date_part("month", "dispensing_date" ) }} as {{ dbt.type_string() }} ),2,'0') as month
       ,cast({{ date_part("year", "dispensing_date" ) }} as {{ dbt.type_string() }} )
            || lpad(cast({{ date_part("month", "dispensing_date" ) }} as {{ dbt.type_string() }} ),2,'0') AS year_month
       ,cast('pharmacy' as {{ dbt.type_string() }}) as claim_type
       ,paid_amount
    from {{ var('pharmacy_claim') }}
)
{% else %}
{% if execute %}
{{- log("pharmacy_claim soruce does not exist, using empty table.", info=true) -}}
{% endif %}
(
    select
        cast(null as {{ dbt.type_string() }} ) as patient_id
       ,cast(null as {{ dbt.type_string() }} ) as year
       ,cast(null as {{ dbt.type_string() }} ) as month
       ,cast(null as {{ dbt.type_string() }} ) as year_month
       ,cast('pharmacy' as {{ dbt.type_string() }}) as claim_type
       ,cast(null as numeric) as paid_amount
    limit 0
)

{%- endif %}


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