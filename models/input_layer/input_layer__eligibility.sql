{% if var('enable_eligibility', false) == true -%}

select *
from {{ ref('eligibility') }}

{% elif var('enable_eligibility', false) == false -%}

{% if target.type == 'fabric' %}
select top 0
      cast(null as {{ dbt.type_string() }} ) as person_id
    , cast(null as {{ dbt.type_string() }} ) as member_id
    , cast(null as {{ dbt.type_string() }} ) as subscriber_id
    , cast(null as {{ dbt.type_string() }} ) as gender
    , cast(null as {{ dbt.type_string() }} ) as race
    , cast(null as date) as birth_date
    , cast(null as date) as death_date
    , cast(null as integer) as death_flag
    , cast(null as date) as enrollment_start_date
    , cast(null as date) as enrollment_end_date
    , cast(null as {{ dbt.type_string() }} ) as payer
    , cast(null as {{ dbt.type_string() }} ) as payer_type
    , cast(null as {{ dbt.type_string() }} ) as {{ quote_column('plan') }}
    , cast(null as {{ dbt.type_string() }} ) as original_reason_entitlement_code
    , cast(null as {{ dbt.type_string() }} ) as dual_status_code
    , cast(null as {{ dbt.type_string() }} ) as medicare_status_code
    , cast(null as {{ dbt.type_string() }} ) as group_id
    , cast(null as {{ dbt.type_string() }} ) as group_name
    , cast(null as {{ dbt.type_string() }} ) as first_name
    , cast(null as {{ dbt.type_string() }} ) as last_name
    , cast(null as {{ dbt.type_string() }} ) as social_security_number
    , cast(null as {{ dbt.type_string() }} ) as subscriber_relation
    , cast(null as {{ dbt.type_string() }} ) as address
    , cast(null as {{ dbt.type_string() }} ) as city
    , cast(null as {{ dbt.type_string() }} ) as state
    , cast(null as {{ dbt.type_string() }} ) as zip_code
    , cast(null as {{ dbt.type_string() }} ) as phone
    , cast(null as {{ dbt.type_string() }} ) as data_source
    , cast(null as {{ dbt.type_string() }} ) as file_name
    , cast(null as date) as file_date
    , cast(null as {{ dbt.type_timestamp() }} ) as ingest_datetime
    , cast(null as {{ dbt.type_timestamp() }} ) as tuva_last_run
{% else %}
select
      cast(null as {{ dbt.type_string() }} ) as person_id
    , cast(null as {{ dbt.type_string() }} ) as member_id
    , cast(null as {{ dbt.type_string() }} ) as subscriber_id
    , cast(null as {{ dbt.type_string() }} ) as gender
    , cast(null as {{ dbt.type_string() }} ) as race
    , cast(null as date) as birth_date
    , cast(null as date) as death_date
    , cast(null as integer) as death_flag
    , cast(null as date) as enrollment_start_date
    , cast(null as date) as enrollment_end_date
    , cast(null as {{ dbt.type_string() }} ) as payer
    , cast(null as {{ dbt.type_string() }} ) as payer_type
    , cast(null as {{ dbt.type_string() }} ) as {{ quote_column('plan') }}
    , cast(null as {{ dbt.type_string() }} ) as original_reason_entitlement_code
    , cast(null as {{ dbt.type_string() }} ) as dual_status_code
    , cast(null as {{ dbt.type_string() }} ) as medicare_status_code
    , cast(null as {{ dbt.type_string() }} ) as group_id
    , cast(null as {{ dbt.type_string() }} ) as group_name
    , cast(null as {{ dbt.type_string() }} ) as first_name
    , cast(null as {{ dbt.type_string() }} ) as last_name
    , cast(null as {{ dbt.type_string() }} ) as social_security_number
    , cast(null as {{ dbt.type_string() }} ) as subscriber_relation
    , cast(null as {{ dbt.type_string() }} ) as address
    , cast(null as {{ dbt.type_string() }} ) as city
    , cast(null as {{ dbt.type_string() }} ) as state
    , cast(null as {{ dbt.type_string() }} ) as zip_code
    , cast(null as {{ dbt.type_string() }} ) as phone
    , cast(null as {{ dbt.type_string() }} ) as data_source
    , cast(null as {{ dbt.type_string() }} ) as file_name
    , cast(null as date) as file_date
    , cast(null as {{ dbt.type_timestamp() }} ) as ingest_datetime
    , cast(null as {{ dbt.type_timestamp() }} ) as tuva_last_run
limit 0
{%- endif %}

{%- endif %}
