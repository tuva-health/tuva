/*
Filter conditions to those that were classified and pick the classification
with the greatest probability (that's the greatest logic). This logic removes
any rows that were not classified.
*/
{{ config(
     enabled = var('claims_enabled', False) | as_bool
   )
}}

select
   a.encounter_id
   , a.primary_diagnosis_code
   , a.primary_diagnosis_code_type
   , a.edcnnpa
   , a.edcnpa
   , a.epct
   , a.noner
   , a.injury
   , a.psych
   , a.alcohol
   , a.drug
   , a.ed_classification_capture
   , case greatest(edcnnpa, edcnpa, epct, noner, injury, psych, alcohol, drug)
          when edcnnpa then 'edcnnpa'
          when edcnpa then 'edcnpa'
          when epct then 'epct'
          when noner then 'noner'
          when injury then 'injury'
          when psych then 'psych'
          when alcohol then 'alcohol'
          when drug then 'drug'
          else 'unclassified'
   end as classification
from {{ ref('ed_classification__map_primary_dx') }} as a
where ed_classification_capture = 1
