

{{ config(
     enabled = var('claims_preprocessing_enabled',var('tuva_packages_enabled',True))
   )
}}




-- *************************************************
-- This dbt model creates the eligibility table in core.
-- *************************************************




select *
from {{ ref('claims_preprocessing__eligibility') }} 
