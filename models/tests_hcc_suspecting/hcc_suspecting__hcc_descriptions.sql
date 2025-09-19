{{ config(materialized='view') }}
select * from {{ ref('hcc_suspecting__hcc_descriptions') }}