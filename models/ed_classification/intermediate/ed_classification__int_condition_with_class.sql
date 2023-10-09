/*
Filter conditions to those that were classified and pick the classification
with the greatest probability (that's the greatest logic). This logic removes
any rows that were not classified.
*/
{{ config(
     enabled = var('ed_classification_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

select
   a.encounter_id
   , a.claim_id
   , a.patient_id
   , a.code_type
   , a.code
   , a.description
   , a.ccs_description_with_covid
   , a.recorded_date
   , cast({{ date_part("year", "recorded_date") }} as {{ dbt.type_string() }}) as recorded_date_year
   , cast({{ date_part("year", "recorded_date") }} as {{ dbt.type_string() }})
     || lpad(cast({{ date_part("month", "recorded_date") }} as {{ dbt.type_string() }}), 2, '0')
     as recorded_date_year_month
   , a.claim_paid_amount_sum
   , case greatest(edcnnpa, edcnpa, epct, noner, injury, psych, alcohol, drug)
          when edcnnpa then 'edcnnpa'
          when edcnpa then 'edcnpa'
          when epct then 'epct'
          when noner then 'noner'
          when injury then 'injury'
          when psych then 'psych'
          when alcohol then 'alcohol'
          when drug then 'drug'
          else 'unclassified'
   end as classification
from {{ ref('ed_classification__int_merge_condition') }} a
where ed_classification_capture = 1
