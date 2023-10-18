/*
Filter conditions to those that were classified and pick the classification
with the greatest probability (that's the greatest logic). This logic removes
any rows that were not classified.
*/
{{ config(
     enabled = var('ed_classification_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

select
   a.*
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
from {{ ref('ed_classification__int_map_primary_dx') }} a
where ed_classification_capture = 1
