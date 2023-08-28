{{ config(
     enabled = var('clinical_enabled',var('tuva_marts_enabled',False))
   )
}}

select * from {{ ref('core__stg_medical_records_lab_result')}}