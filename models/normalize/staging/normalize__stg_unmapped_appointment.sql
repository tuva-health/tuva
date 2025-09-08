{{ config(
     enabled = ( ( var('enable_normalize_engine', False) == True  or  var('enable_normalize_engine', False) == "unmapped") and
                   var('clinical_enabled', var('tuva_marts_enabled', False))
               ) | as_bool
   )
}}

with appointments as (

    select * from {{ ref('core__appointment') }}

)

, appointment_type as (

    select distinct
          'appointment_type' as source_code_type
        , source_appointment_type_code as source_code
        , source_appointment_type_description as source_description
        , 'appointment_type' as normalized_code_type
        , normalized_appointment_type_code as normalized_code
        , normalized_appointment_type_description as normalized_description
        , data_source
    from appointments

)

, appointment_status as (

    select distinct
          'appointment_status' as source_code_type
        , source_status as source_code
        , source_status as source_description
        , 'appointment_status' as normalized_code_type
        , normalized_status as normalized_code
        , normalized_status as normalized_description
        , data_source
    from appointments

)

, reason_code as (

    select distinct
          source_reason_code_type as source_code_type
        , source_reason_code as source_code
        , source_reason_description as source_description
        , normalized_reason_code_type as normalized_code_type
        , normalized_reason_code as normalized_code
        , normalized_reason_description as normalized_description
        , data_source
    from appointments

)

, appointment_cancellation_reason as (

    select distinct
          source_cancellation_reason_code_type as source_code_type
        , source_cancellation_reason_code as source_code
        , source_cancellation_reason_description as source_description
        , normalized_cancellation_reason_code_type as normalized_code_type
        , normalized_cancellation_reason_code as normalized_code
        , normalized_cancellation_reason_description as normalized_description
        , data_source
    from appointments

)

, unioned as (

    select * from appointment_type
    union all
    select * from appointment_status
    union all
    select * from reason_code
    union all
    select * from appointment_cancellation_reason

)

{% if var('enable_normalize_engine',false) == True %}

select
      unioned.source_code_type
    , unioned.source_code
    , unioned.source_description
    , count(*) as item_count
    , 'appointment' as domain
    , unioned.data_source
from unioned
    left join {{ ref('custom_mapped') }} as custom_mapped
        on (lower(unioned.source_code_type) = lower(custom_mapped.source_code_type)
            or (unioned.source_code_type is null and custom_mapped.source_code_type is null)
        )
        and (unioned.source_code = custom_mapped.source_code
            or (unioned.source_code is null and custom_mapped.source_code is null)
        )
        and (unioned.source_description = custom_mapped.source_description
            or (unioned.source_description is null and custom_mapped.source_description is null)
        )
where unioned.normalized_code is null and unioned.normalized_description is null
    and not (unioned.source_code is null and unioned.source_description is null)
    and custom_mapped.not_mapped is null
group by
      unioned.source_code_type
    , unioned.source_code
    , unioned.source_description
    , unioned.data_source

{% else %}

select
      unioned.source_code_type
    , unioned.source_code
    , unioned.source_description
    , count(*) as item_count
    , 'appointment' as domain
    , unioned.data_source
from unioned
where unioned.normalized_code is null and unioned.normalized_description is null
    and not (unioned.source_code is null and unioned.source_description is null)
group by
      unioned.source_code_type
    , unioned.source_code
    , unioned.source_description
    , unioned.data_source

{% endif %}