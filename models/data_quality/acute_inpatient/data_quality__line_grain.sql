{{ config(
    enabled = var('claims_enabled', False)
) }}

select 
      claim_id
    , claim_line_number

    -- Fields that are considered 'institutional' are fields that
    -- should only be on institutional claims and not on professional claims.
    -- That means that if at least one 'institutional' field is present on the claim
    -- we should label that claim as an institutional claim.
    -- 'Institutional' fields are the following:
    --      - bill_type_code
    --      - drg_code
    --      - admit_type_code
    --      - admit_source_code
    --      - discharge_disposition_code
    --      - revenue_center_code
    -- Note that we could have considered 'admission_date' and 'discharge_date'
    -- to be 'institutional' fields, since only institutional claims should
    -- have admission and discharge dates and we would expect to not see those
    -- dates on professional claims. In practice we have seen many professional
    -- claims with admission and discharge dates, so we are not including
    -- admission_date and discharge_date in our list of 'institutional' fields.
    , case
          when (bill_type_code is not null 
                or drg_code is not null
                or admit_type_code is not null 
                or admit_source_code is not null 
                or discharge_disposition_code is not null 
                or revenue_center_code is not null)  -- or
                -- admission_date is not null or
                -- discharge_date is not null
          then 1
          else 0
      end as has_institutional_fields

    , case
          when (valid_bill_type_code = 1 
                or valid_drg_code = 1
                or valid_admit_type_code = 1 
                or valid_admit_source_code = 1 
                or valid_discharge_disposition_code = 1 
                or valid_revenue_center_code = 1)  -- or
                -- valid_admission_date = 1 or
                -- valid_discharge_date = 1
          then 1
          else 0
      end as has_valid_institutional_fields

    -- A 'professional' field is a field that should only be on professional claims
    -- and not on institutional claims.
    -- There is only one such field: place_of_service_code.
    -- Since in practice we often see place_of_service_code imputed on institutional
    -- claims, we only label a claim to be a professional claim if it has
    -- a place_of_service_code but NO 'institutional' fields. If a claim
    -- has place_of_service_code and also at least one 'institutional' field
    -- then we label that claim to be 'institutional'.
    , case
          when place_of_service_code is not null then 1
          else 0
      end as has_professional_fields

    , case
          when valid_place_of_service_code = 1 then 1
          else 0
      end as has_valid_professional_fields
      , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('data_quality__valid_values') }}
