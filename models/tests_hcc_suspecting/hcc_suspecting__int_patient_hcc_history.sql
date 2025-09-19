{{ config(materialized='view') }}
select * from {{ ref('hcc_suspecting__int_patient_hcc_history') }}