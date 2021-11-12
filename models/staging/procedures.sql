
{{ config(materialized='view') }}

select
    cast(encounter_id as string) as encounter_id,
    cast(procedure_code as string) as procedure_code,
    cast(procedure_code_ranking as integer) as procedure_code_ranking
from hcup.public.procedures
