{% set number_of_columns = 25 %}
{% set column_prefix = 'diagnosis_code_' %}

{% for i in range(1, number_of_columns + 1) %}


with clean_header as(
  select
      claim_id
      , diagnosis_code_type
      , replace(diagnosis_code_1,'','') as diagnosis_code_1
      , replace(diagnosis_code_2,'','') as diagnosis_code_2
      , replace(diagnosis_code_3,'','') as diagnosis_code_3
      , data_source
  from {{ ref('medical_claim') }}
)

, normalize_header as(
  select 
    claim_id
    , source_value
    , coalesce(term_9.icd_9_cm, term_10.icd_10_cm) as normalized_value
    , data_source
    , count(*) as header_occurrence_count
   from clean_header med
    left join wellbe.terminology.icd_10_cm term_10
    on med.diagnosis_code_1 = term_10.icd_10_cm
    and med.diagnosis_code_type = 'icd-10-cm'
  left join wellbe.terminology.icd_9_cm term_9
    on med.diagnosis_code_1 = term_9.icd_9_cm
    and med.diagnosis_code_type = 'icd-9-cm'
  group by
    claim_id
    , diagnosis_code_type
    , diagnosis_code_1
    , coalesce(term_9.icd_9_cm, term_10.icd_10_cm)
    , data_source
)
, claim_with_multiple_header_values as(
  select 
    claim_id
    , count(distinct normalized_value) as header_occurrence_count
  from normalize_header med
  group by
    claim_id
    having count(*) > 1
  
)
, claim_with_unique_header_values as(
  select 
    claim_id
    , count(distinct normalized_value) as header_occurrence_count
  from normalize_header med
  group by
    claim_id
  having count(*) = 1
  
)
, occurrence_counts as(
  select 
    norm.claim_id
    , norm.header_value
    , norm.normalized_value
    , norm.header_occurrence_count
    , coalesce(lead(norm.header_occurrence_count) over(partition by norm.claim_id order by norm.header_occurrence_count desc),0) as next_occurrence_count
    , row_number() over (partition by norm.claim_id order by norm.header_occurrence_count desc) as occurrence_row_count
  from normalize_header norm
  inner join claim_with_multiple_header_values multi
    on norm.claim_id = multi.claim_id
)
  ,voting_determinable as(
  select
    *
  from occurrence_counts
  where occurrence_row_count = 1
  and header_occurrence_count > next_occurrence_count
 )
 
  select
    claim_id
    , header_value
    , normalized_value as diagnosis_code_1
  from voting_determinable
 
 union all

  select 
    norm.claim_id
    , header_value
    , norm.normalized_value as diagnosis_code_1
  from normalize_header norm
  inner join claim_with_unique_header_values uniq
    on norm.claim_id = uniq.claim_id


{% endfor %}