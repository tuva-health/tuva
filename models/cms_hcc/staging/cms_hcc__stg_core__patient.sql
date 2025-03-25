{{ config(
     enabled = var('cms_hcc_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}
select
      person_id
    , sex
    , birth_date
    , death_date
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('core__patient') }}
