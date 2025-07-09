select
    med.medical_claim_sk
    , enc.old_encounter_id
from {{ ref('encounters__int_ambulance__generate_encounter_id') }} as enc
    inner join {{ ref('encounters__stg_medical_claim') }} med
    on med.patient_data_source_id = enc.patient_data_source_id
    and med.claim_id = enc.claim_id