

{{ config(
     enabled = var('claims_preprocessing_enabled',var('tuva_packages_enabled',True))
   )
}}




-- *************************************************
-- This dbt model creates the pharmacy_claim
-- table in core.
-- *************************************************




select *
from {{ ref('input_layer__pharmacy_claim') }} 
