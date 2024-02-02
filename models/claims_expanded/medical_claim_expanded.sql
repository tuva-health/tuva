
{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
   )
}}


select *
from {{ ref('medical_claim') }}
