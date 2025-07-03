select
    condition_sk
    , medical_claim_sk
    , data_source
    , claim_id
    , member_id
    , recorded_date
    , rank_num
    , source_code_type
    , source_code
    , normalized_code_type
    , normalized_code
    , present_on_admit_code
    , {{ current_timestamp() }} as tuva_last_run
from {{ ref('the_tuva_project', 'core__stg_claims_condition') }}