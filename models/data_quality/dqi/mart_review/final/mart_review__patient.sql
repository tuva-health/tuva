
select *
    , {{ concat_custom([
        'person_id',
        "'|'",
        'data_source']) }} as patient_data_source_key
from {{ ref('core__patient') }}
