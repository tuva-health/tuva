{{ config(
     enabled = var('clinical_enabled',var('tuva_marts_enabled',False)) | as_bool
   )
}}

select * from {{ ref('core__stg_clinical_lab_result')}}