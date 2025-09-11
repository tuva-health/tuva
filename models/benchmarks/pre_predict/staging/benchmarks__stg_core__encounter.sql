{# {{
    config(
        enabled = var('benchmarks_train', False) | as_bool
    )
}} #}

select
    encounter_id
    , person_id
    , data_source
    , encounter_start_date
    , encounter_type
    , encounter_group
    , length_of_stay
    , discharge_disposition_code
    , facility_id
    , facility_name
    , drg_code_type
    , drg_code
    , drg_description
    , paid_amount
    , primary_diagnosis_code
from {{ ref('core__encounter') }}
