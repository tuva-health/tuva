select
      person_id
    , enrollment_start_date
    , enrollment_end_date
    , original_reason_entitlement_code
    , dual_status_code
    , medicare_status_code
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('core__eligibility') }}
