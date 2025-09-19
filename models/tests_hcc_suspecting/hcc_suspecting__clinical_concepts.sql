{{ config(materialized='view') }}
select * from {{ ref('hcc_suspecting__clinical_concepts') }}