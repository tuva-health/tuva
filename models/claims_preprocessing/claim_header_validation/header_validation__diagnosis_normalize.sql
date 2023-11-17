
with clean_header as(
    select
        claim_id
        , diagnosis_code_type
        , replace(diagnosis_code_1,'','') as diagnosis_code_1
        , replace(diagnosis_code_2,'','') as diagnosis_code_2
        , replace(diagnosis_code_3,'','') as diagnosis_code_3
        , replace(diagnosis_code_4,'','') as diagnosis_code_4
        , replace(diagnosis_code_5,'','') as diagnosis_code_5
        , data_source
    from {{ ref('medical_claim') }}
)


select 
    claim_id
    , source_value
    , coalesce(term_9.icd_9_cm, term_10.icd_10_cm) as normalized_value
    , data_source
    , count(*) as header_occurrence_count
from clean_header med
left join {{ ref('terminology__icd_10_cm') }}  term_10
    on med.diagnosis_code_1 = term_10.icd_10_cm
    and med.diagnosis_code_type = 'icd-10-cm'
left join left join {{ ref('terminology__icd_10_cm') }} term_9
    on med.diagnosis_code_1 = term_9.icd_9_cm
    and med.diagnosis_code_type = 'icd-9-cm'
group by
    claim_id
    , diagnosis_code_type
    , diagnosis_code_1
    , coalesce(term_9.icd_9_cm, term_10.icd_10_cm)
    , data_source
