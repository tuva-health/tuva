{{ config(
     enabled = var('clinical_enabled', False)
 | as_bool
   )
}}

{%- set tuva_core_columns -%}
      cast(immunization_id as {{ dbt.type_string() }}) as immunization_id
    , cast(person_id as {{ dbt.type_string() }}) as person_id
    , cast(patient_id as {{ dbt.type_string() }}) as patient_id
    , cast(encounter_id as {{ dbt.type_string() }}) as encounter_id
    , cast(source_code_type as {{ dbt.type_string() }}) as source_code_type
    , cast(source_code as {{ dbt.type_string() }}) as source_code
    , cast(source_description as {{ dbt.type_string() }}) as source_description
    , cast(status as {{ dbt.type_string() }}) as status
    , cast(status_reason as {{ dbt.type_string() }}) as status_reason
    , {{ try_to_cast_date('occurrence_date', 'YYYY-MM-DD') }} as occurrence_date
    , cast(source_dose as {{ dbt.type_string() }}) as source_dose
    , cast(lot_number as {{ dbt.type_string() }}) as lot_number
    , cast(body_site as {{ dbt.type_string() }}) as body_site
    , cast(route as {{ dbt.type_string() }}) as route
    , cast(location_id as {{ dbt.type_string() }}) as location_id
    , cast(practitioner_id as {{ dbt.type_string() }}) as practitioner_id
{%- endset -%}

{%- set tuva_metadata_columns -%}
    , cast(ingest_datetime as {{ dbt.type_timestamp() }}) as ingest_datetime
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
    , cast(data_source as {{ dbt.type_string() }}) as data_source
{%- endset -%}

with immune as (

    select
        {{ tuva_core_columns }}
        {{ select_extension_columns(ref('input_layer__immunization'), strip_prefix=false) }}
        {{ tuva_metadata_columns }}
    from {{ ref('input_layer__immunization') }}

)

select
      immune.immunization_id
    , immune.person_id
    , immune.patient_id
    , immune.encounter_id
    , immune.source_code_type
    , immune.source_code
    , immune.source_description
    , case
        when cvx.cvx is not null then 'cvx'
        else null end as normalized_code_type
    , cvx.cvx as normalized_code
    , cvx.long_description as normalized_description
    , coalesce(immunization_status.status, immune.status) as status
    , coalesce(immunization_status_reason.description, immune.status_reason) as status_reason
    , immune.occurrence_date
    , immune.source_dose
    , cast(null as {{ dbt.type_string() }}) as normalized_dose
    , immune.lot_number
    , coalesce(act_site.description, immune.body_site) as body_site
    , coalesce(immunization_route.description, immune.route) as route
    , immune.location_id
    , immune.practitioner_id
    {{ select_extension_columns(ref('input_layer__immunization'), alias='immune', strip_prefix=false) }}
    , immune.ingest_datetime
    , immune.tuva_last_run
    , immune.data_source
from immune
left outer join {{ ref('terminology__cvx') }} as cvx
    on lower(immune.source_code_type) = 'cvx'
        and immune.source_code = cvx.cvx
left outer join {{ ref('terminology__immunization_status') }} as immunization_status
    on immune.status = immunization_status.status_code
left outer join {{ ref('terminology__immunization_status_reason') }} as immunization_status_reason
    on immune.status_reason = immunization_status_reason.reason_code
        and (
        immunization_status_reason.code_type = 'actreason' or
        immunization_status_reason.code_type = 'snomed-ct'
        )
left outer join {{ ref('terminology__act_site') }} as act_site
    on immune.body_site = act_site.body_code
left outer join {{ ref('terminology__immunization_route_code') }} as immunization_route
    on immune.route = immunization_route.route_code
