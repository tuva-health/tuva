{{ config(
     enabled = var('hcc_suspecting_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

select
      person_id
    , payer
    , data_source
    , hcc_code
    , hcc_description
    , reason
    , contributing_factor
    , suspect_date
    , tuva_last_run
from {{ref('hcc_suspecting__list_all')}}
    {% if target.type == 'fabric' %}
        where (current_year_billed = 0
            or current_year_billed is null)
    {% else %}
        where (current_year_billed = false
            or current_year_billed is null)
    {% endif %}