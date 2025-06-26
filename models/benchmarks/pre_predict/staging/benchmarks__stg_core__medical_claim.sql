select
    person_id,
    encounter_id,
    paid_amount,
    data_source,
    payer,
    {{ quote_column('plan') }},
    claim_end_date,
    encounter_type,
    encounter_group,
    enrollment_flag
from {{ ref('core__medical_claim') }}