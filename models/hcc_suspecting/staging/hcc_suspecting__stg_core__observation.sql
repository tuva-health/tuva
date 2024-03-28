{{ config(
     enabled = var('hcc_suspecting_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}
select
      patient_id
    , observation_date
    , result
    , coalesce(normalized_code_type,source_code_type) as code_type
    , coalesce(normalized_code,source_code) as code
    , data_source
from {{ ref('core__observation') }}