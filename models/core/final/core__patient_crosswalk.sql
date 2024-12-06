{{ config(
     enabled = var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))
 | as_bool
   )
}}

{% if var('clinical_enabled', var('tuva_marts_enabled',False)) == true and var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

select distinct
      person_id
    , null as patient_id
    , member_id
    , payer
    , plan
    , data_source
from {{ ref('normalized_input__eligibility')}}
union all
select distinct
      person_id
    , patient_id
    , null as member_id
    , null as payer
    , null as plan
    , data_source
from {{ ref('patient') }}

{% elif var('clinical_enabled', var('tuva_marts_enabled',False)) == true -%}

select distinct
      person_id
    , patient_id
    , null as member_id
    , null as payer
    , null as plan
    , data_source
from {{ ref('patient') }}from {{ ref('patient') }}

{% elif var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

select distinct
      person_id
    , null as patient_id
    , member_id
    , payer
    , plan
    , data_source
from {{ ref('normalized_input__eligibility')}}

{%- endif %}