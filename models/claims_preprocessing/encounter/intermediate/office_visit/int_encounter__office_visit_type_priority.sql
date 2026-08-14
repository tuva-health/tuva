{{ config(
     enabled = var('claims_enabled', False) | as_bool
   )
}}


select claim_id
, claim_line_number
, old_encounter_id
, 'office visit radiology' as encounter_type
, 0 as priority_number
from {{ ref('int_encounter__office_visit_type_radiology') }}

{% if target.type == 'fabric' %}
union
{% else %}
union distinct
{% endif %}

select claim_id
, claim_line_number
, old_encounter_id
, 'office visit surgery' as encounter_type
, 1 as priority_number
from {{ ref('int_encounter__office_visit_type_surgery') }}

{% if target.type == 'fabric' %}
union
{% else %}
union distinct
{% endif %}

select claim_id
, claim_line_number
, old_encounter_id
, 'office visit injections' as encounter_type
, 2 as priority_number
from {{ ref('int_encounter__office_visit_type_injection') }}

{% if target.type == 'fabric' %}
union
{% else %}
union distinct
{% endif %}

select claim_id
, claim_line_number
, old_encounter_id
, 'office visit pt/ot/st' as encounter_type
, 3 as priority_number
from {{ ref('int_encounter__office_visit_type_therapy') }}

{% if target.type == 'fabric' %}
union
{% else %}
union distinct
{% endif %}

select claim_id
, claim_line_number
, old_encounter_id
, 'office visit' as encounter_type
, 4 as priority_number
from {{ ref('int_encounter__office_visit_type_em') }}

{% if target.type == 'fabric' %}
union
{% else %}
union distinct
{% endif %}

select claim_id
, claim_line_number
, old_encounter_id
, 'telehealth' as encounter_type
, 5 as priority_number
from {{ ref('int_encounter__office_visit_type_telehealth') }}

{% if target.type == 'fabric' %}
union
{% else %}
union distinct
{% endif %}

select claim_id
, claim_line_number
, old_encounter_id
, 'office visit - other' as encounter_type
, 9999 as priority_number
from {{ ref('int_encounter__office_visit_candidate') }}
