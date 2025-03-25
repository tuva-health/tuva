{{ config(
     enabled = var('cms_chronic_conditions_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

{% if target.type == 'fabric' %}
with conditions_unioned as (

    select * from {{ ref('chronic_conditions__cms_chronic_conditions_all') }}
    union
    select * from {{ ref('chronic_conditions__cms_chronic_conditions_hiv_aids') }}
    union
    select * from {{ ref('chronic_conditions__cms_chronic_conditions_oud') }}

)
{% else %}
with conditions_unioned as (

    select * from {{ ref('chronic_conditions__cms_chronic_conditions_all') }}
    union distinct
    select * from {{ ref('chronic_conditions__cms_chronic_conditions_hiv_aids') }}
    union distinct
    select * from {{ ref('chronic_conditions__cms_chronic_conditions_oud') }}

)
{% endif %}

select
      person_id
    , claim_id
    , start_date
    , chronic_condition_type
    , condition_category
    , condition
    , data_source
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from conditions_unioned
