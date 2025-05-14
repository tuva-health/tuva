{{ config(
     enabled = var('fhir_preprocessing_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}
select distinct
      cast(person_id as {{ dbt.type_string() }} ) as patient_internal_id
    , cast(procedure_id as {{ dbt.type_string() }} ) as resource_internal_id
    , 'completed' as procedure_status
    , case
        when lower(
            coalesce(
                  cast(normalized_code_type as {{ dbt.type_string() }} )
                , cast(source_code_type as {{ dbt.type_string() }} )
            )
        ) = 'icd-10-pcs' then 'ICD10'
        when lower(
            coalesce(
                  cast(normalized_code_type as {{ dbt.type_string() }} )
                , cast(source_code_type as {{ dbt.type_string() }} )
            )
        ) = 'icd-9-pcs' then 'ICD9'
        else coalesce(
              cast(normalized_code_type as {{ dbt.type_string() }} )
            , cast(source_code_type as {{ dbt.type_string() }} )
        )
      end as procedure_code_system
    , coalesce(
          cast(normalized_code as {{ dbt.type_string() }} )
        , cast(source_code as {{ dbt.type_string() }} )
      ) as procedure_code
    , coalesce(
          cast(normalized_description as {{ dbt.type_string() }} )
        , cast(source_description as {{ dbt.type_string() }} )
      ) as procedure_display
    , cast(procedure_date as {{ dbt.type_timestamp() }} ) as procedure_performed_datetime
    , cast(practitioner_id as {{ dbt.type_string() }} ) as practitioner_npi
    , cast(data_source as {{ dbt.type_string() }} ) as data_source
from {{ ref('fhir_preprocessing__stg_core__procedure') }}
where procedure_id is not null
and normalized_code_type is not null
and claim_id is null /* claim procedures are included in the EOB resource */