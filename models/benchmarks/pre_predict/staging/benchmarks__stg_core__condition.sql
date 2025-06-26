select
    person_id,
    normalized_code,
    recorded_date
from {{ ref('core__condition') }}