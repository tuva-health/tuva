with exclusions as (

    select * from quality_measures__int_nqf0053_exclude_procedures_medications

)

select
      patient_id
    , exclusion_date
    , exclusion_reason
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from
    exclusions

-- update on schema.yml