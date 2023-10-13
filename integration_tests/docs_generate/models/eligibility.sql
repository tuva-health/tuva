select patient_id,
       member_id,
       birth_date,
       death_date,
       enrollment_start_date,
       enrollment_end_date,
       payer,
       payer_type,
       original_reason_entitlement_code,
       dual_status_code,
       medicare_status_code,
       data_source
from tuva._tuva_synthetic.eligibility_seed
