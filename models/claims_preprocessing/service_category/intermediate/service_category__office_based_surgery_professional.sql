{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

{# Since HCPCS is a string, we need to exclude values that would fall in our comparison range for alphanumeric values #}
with numeric_hcpcs as (
    select *
    from {{ ref('service_category__stg_medical_claim') }} as med
    {% if target.type in ('duckdb', 'databricks') %}
        where try_cast('hcpcs_code' as integer) is not null
    {% else %}
        where {{ safe_cast('hcpcs_code', 'int') }} is not null
    {% endif %}
)


    select distinct
        med.claim_id
      , med.claim_line_number
      , med.claim_line_id
      , 'office-based' as service_category_1
      , 'office-based surgery' as service_category_2
      , 'office-based surgery' as service_category_3
      , '{{ this.name }}' as source_model_name
      , '{{ var('tuva_last_run') }}' as tuva_last_run
    from numeric_hcpcs as med
    inner join {{ ref('service_category__stg_office_based') }} as prof
      on med.claim_id = prof.claim_id
      and med.claim_line_number = prof.claim_line_number
    where
      (hcpcs_code between '10021' and '69999')
