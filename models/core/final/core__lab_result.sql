{{ config(
     enabled = var('clinical_enabled',var('tuva_marts_enabled',False))
   )
}}

select * from {{ ref('core__stg_clinical_lab_result')}}