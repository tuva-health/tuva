
{{ config(materialized='view') }}

select
    cast(encounter_id as string) as encounter_id,
    cast(diagnosis_code as string) as diagnosis_code,
    cast(diagnosis_code_ranking as integer) as diagnosis_code_ranking,
    cast(present_on_admission_code as integer) as present_on_admission_code
from hcup.public.diagnoses
