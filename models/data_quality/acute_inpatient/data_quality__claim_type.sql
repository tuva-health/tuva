{{ config(
    enabled = var('claims_enabled', False)
) }}

with claim_grain_labels as (
      select
            claim_id  
          , max(has_institutional_fields) as has_institutional_fields
          , max(has_valid_institutional_fields) as has_valid_institutional_fields  
          , max(has_professional_fields) as has_professional_fields
          , max(has_valid_professional_fields) as has_valid_professional_fields
      from {{ ref('data_quality__line_grain') }}
      group by claim_id
  ),


  add_calculated_claim_type as (
      select
            claim_id

            -- Determine a calculated_claim_type using this logic:
            --   - A claim is 'institutional' if it has at least one institutional field
            --   - A claim is 'professional' if it has NO institutional fields and
            --     has at least one professional field.
            --   - A claim is 'undetermined' if it has NO institutional fields and
            --     NO professional fields
            , case
                  when has_institutional_fields = 1 then 'institutional'
                  when (has_institutional_fields = 0) 
                    and (has_professional_fields = 1) then 'professional'
                  else 'undetermined'
              end as calculated_claim_type

            , has_institutional_fields
            , has_valid_institutional_fields
            , has_professional_fields
            , has_valid_professional_fields
      from claim_grain_labels
  )


select 
      claim_id
    , calculated_claim_type
    , has_institutional_fields
    , has_valid_institutional_fields
    , has_professional_fields
    , has_valid_professional_fields
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_calculated_claim_type
