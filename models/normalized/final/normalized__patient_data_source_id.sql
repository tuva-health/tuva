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
/* Rank on the columns themselves rather than on a concatenation of them.
   concat() with no separator loses the boundary between the two values, so
   ('A','BC') and ('AB','C') both produce the key 'ABC' and dense_rank hands
   two different patients the same patient_data_source_id. */
, dense_rank() over (
order by person_id, data_source) as patient_data_source_id
from multiple_sources
