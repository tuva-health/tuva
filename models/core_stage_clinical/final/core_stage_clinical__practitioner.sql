select
    practitioner_id
    , npi
    , first_name
    , last_name
    , practice_affiliation
    , specialty
    , sub_specialty
    , data_source
    , tuva_last_run
from {{ ref('core_stage_clinical__practitioner') }}