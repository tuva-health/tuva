{{ config(
     enabled = (var('claims_enabled', False) | string | lower) == 'true'
   )
}}

with multiple_sources as (
select distinct person_id
, data_source
from {{ ref('normalized__medical_claim') }}

{% if target.type == 'fabric' %}
union
{% else %}
union distinct
{% endif %}

select distinct person_id
, data_source
from {{ ref('normalized__eligibility') }}
)

select
person_id
, data_source
/* Hash the key columns rather than ranking them. dbt_utils delimits each value,
   so the two cannot run together into a colliding key. Dropping dense_rank also
   removes a global sort over every person in the warehouse. */
, {{ dbt_utils.generate_surrogate_key(['person_id', 'data_source']) }} as patient_data_source_id
from multiple_sources
