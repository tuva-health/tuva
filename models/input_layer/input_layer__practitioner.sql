{% if var('enable_practitioner', false) == true -%}

select *
from {{ ref('practitioner') }}

{% elif var('enable_practitioner', false) == false -%}

{% if target.type == 'fabric' %}
select top 0
      cast(null as {{ dbt.type_string() }} ) as practitioner_id
    , cast(null as {{ dbt.type_string() }} ) as npi
    , cast(null as {{ dbt.type_string() }} ) as first_name
    , cast(null as {{ dbt.type_string() }} ) as last_name
    , cast(null as {{ dbt.type_string() }} ) as practice_affiliation
    , cast(null as {{ dbt.type_string() }} ) as specialty
    , cast(null as {{ dbt.type_string() }} ) as sub_specialty
    , cast(null as {{ dbt.type_string() }} ) as data_source
    , cast(null as {{ dbt.type_timestamp() }} ) as tuva_last_run
{% else %}
select
      cast(null as {{ dbt.type_string() }} ) as practitioner_id
    , cast(null as {{ dbt.type_string() }} ) as npi
    , cast(null as {{ dbt.type_string() }} ) as first_name
    , cast(null as {{ dbt.type_string() }} ) as last_name
    , cast(null as {{ dbt.type_string() }} ) as practice_affiliation
    , cast(null as {{ dbt.type_string() }} ) as specialty
    , cast(null as {{ dbt.type_string() }} ) as sub_specialty
    , cast(null as {{ dbt.type_string() }} ) as data_source
    , cast(null as {{ dbt.type_timestamp() }} ) as tuva_last_run
limit 0
{%- endif %}

{%- endif %}
