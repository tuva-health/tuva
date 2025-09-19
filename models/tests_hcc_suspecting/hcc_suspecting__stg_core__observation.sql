{{ config(materialized='view') }}
select * from {{ ref('hcc_suspecting__stg_core__observation') }}