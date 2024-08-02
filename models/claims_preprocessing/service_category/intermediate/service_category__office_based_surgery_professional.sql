{{ config(
    enabled = var('claims_preprocessing_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
) }}

{# Since HCPCS is a string, we need to exclude values that would fall in our comparison range for alphanumeric values #}
with numeric_hcpcs as (
    select *
    from {{ ref('service_category__stg_medical_claim') }} as med
    where {{ dbt_utils.is_numeric('hcpcs_code') }}
)

, final as (
    select distinct 
        med.claim_id
      , med.claim_line_number
      , med.claim_line_id
      , 'Office-Based Surgery' as service_category_2
      , 'Office-Based Surgery' as service_category_3
      , '{{ this.name }}' as source_model_name
      , '{{ var('tuva_last_run') }}' as tuva_last_run
    from numeric_hcpcs as med
    inner join {{ ref('service_category__stg_professional') }} as prof
      on med.claim_id = prof.claim_id
      and med.claim_line_number = prof.claim_line_number
    where 
      (hcpcs_code between '10021' and '69999')
      or (hcpcs_code between '90281' and '99091')
)

select * from final