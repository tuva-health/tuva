{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
 | as_bool
   )
}}

select *    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('quality_measures__measures') }} as p
