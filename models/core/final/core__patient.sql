{{ config(
     enabled = var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))
 | as_bool
   )
}}

{% if var('clinical_enabled', var('tuva_marts_enabled',False)) == true and var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

with person_list_to_exclude_because_in_claims as (
    select distinct person_id
    from {{ ref('core__stg_claims_patient') }}
)
select *
from {{ ref('core__stg_claims_patient') }}
union all
select cscp.*
from {{ ref('core__stg_clinical_patient') }} as cscp
left outer join person_list_to_exclude_because_in_claims as pltebic 
    on  cscp.person_id = pltebic.person_id
/* IF EXISTS IN CLAIMS, CHOOSE CLAIMS RECORD OVER CLINICAL RECORD */
where pltebic.person_id is null

{% elif var('clinical_enabled', var('tuva_marts_enabled',False)) == true -%}

select * from {{ ref('core__stg_clinical_patient') }}

{% elif var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

select * from {{ ref('core__stg_claims_patient') }}

{%- endif %}
