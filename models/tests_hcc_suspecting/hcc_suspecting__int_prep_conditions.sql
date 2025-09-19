{{ config(materialized='view') }}
select * from {{ ref('hcc_suspecting__int_prep_conditions') }}