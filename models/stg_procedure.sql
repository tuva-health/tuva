{{ config(materialized='table', tags='core') }}

select
    encounter_id
,   code_type
,   procedure_code 
from {{ var('src_procedure') }}
