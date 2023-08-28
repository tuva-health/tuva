{{ config(
     enabled = var('medical_records_enabled',var('tuva_marts_enabled',False))
   )
}}


select * from {{ ref('core__stg_medical_records_medication')}}