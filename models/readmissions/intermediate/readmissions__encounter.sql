{{ config(
     enabled = var('readmissions_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

-- Staging model for the input layer:
-- stg_encounter input layer model.
-- This contains one row for every unique encounter in the dataset.

select
    cast(encounter_id as {{ dbt.type_string() }}) as encounter_id,
    cast(patient_id as {{ dbt.type_string() }}) as patient_id,
    cast(encounter_start_date as date) as admit_date,
    cast(encounter_end_date as date) as discharge_date,
    cast(discharge_disposition_code as {{ dbt.type_string() }}) as discharge_disposition_code,
    cast(facility_npi as {{ dbt.type_string() }}) as facility_npi,
    cast(ms_drg_code as {{ dbt.type_string() }}) as ms_drg_code,
    cast(paid_amount as numeric) as paid_amount,
    cast(primary_diagnosis_code as {{ dbt.type_string() }}) as primary_diagnosis_code,
    '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('readmissions__stg_core__encounter') }}
