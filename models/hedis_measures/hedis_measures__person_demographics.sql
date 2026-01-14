{{ config(
     enabled = (  var('hedis_measures_enabled', False) == True  and
                  (var('claims_enabled', False) == True or var('clinical_enabled', False) == True)
               ) | as_bool
   )
}}

select
  person_id
, sex
, birth_date
, death_date
, race
, ethnicity
from {{ ref('core__patient') }}
