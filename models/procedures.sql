
{{ config(materialized='view') }}

select
    encounter_id
,   code_type
,   procedure_code 
from {{ var('src_procedures') }}
