/*
All condition discharge diagnosis left join with probabilistic
indicators of ED classification terminology
*/

{{ config(
     enabled = var('ed_classification_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}
{% set colnames = ["edcnnpa", "edcnpa", "epct", "noner", "injury", "psych", "alcohol", "drug"] %}

with condition as (
    select
        encounter_id
        , primary_diagnosis_code
        , primary_diagnosis_code_type
    from {{ ref('core__encounter') }}
    where encounter_type = 'emergency department'
)

, icd9 as (
  select
     icd9 as code
     {% for colname in colnames %}
     , {{ colname }}
     {% endfor %}
     , 1 as ed_classification_capture
  from {{ ref('ed_classification__johnston_icd9') }}
)

, icd10 as (
  select
     icd10 as code
     {% for colname in colnames %}
     , {{ colname }}
     {% endfor %}
     , 1 as ed_classification_capture
  from {{ ref('ed_classification__johnston_icd10') }}
)

select
   a.encounter_id
   , a.primary_diagnosis_code
   , a.primary_diagnosis_code_type
   {% for colname in colnames %}
   , icd10.{{ colname }}
   {% endfor %}
   , coalesce(icd10.ed_classification_capture, 0) as ed_classification_capture
from condition as a
left outer join icd10
    on a.primary_diagnosis_code = icd10.code
    and a.primary_diagnosis_code_type = 'icd-10-cm'

union all

select
   a.encounter_id
   , a.primary_diagnosis_code
   , a.primary_diagnosis_code_type
   {% for colname in colnames %}
   , icd9.{{ colname }}
   {% endfor %}
   , coalesce(icd9.ed_classification_capture, 0) as ed_classification_capture
from condition as a
inner join icd9
    on a.primary_diagnosis_code = icd9.code
    and a.primary_diagnosis_code_type = 'icd-9-cm'
