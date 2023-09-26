{{ config(
     enabled = var('ed_classification_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

select
    encounter_id
    , encounter_type
from {{ ref('core__encounter') }}