
{{ config(materialized='view') }}

select
    encounter_id
,   procedure_code_type
,   procedure_code 
from {{ var('stg_procedures') }}
