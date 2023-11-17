with clean_header as(
  select
      claim_id
      , diagnosis_code_type
      , replace(diagnosis_code_1,'.','') as diagnosis_code_1
      , replace(diagnosis_code_2,'.','') as diagnosis_code_2
      , replace(diagnosis_code_3,'.','') as diagnosis_code_3
      , data_source
  from wellbe._input_layer_stage.aetna_medical_transform_mappings
  where claim_id in ('EZJMYSSMS02', 'EJY1X3GCV00','EDPCNPTB400')
)

  select 
  claim_id
    , diagnosis_code_type
    , med.diagnosis_code_1 as diagnosis_code_1_source
    , coalesce(diag_1_9.icd_9_cm, diag_1_10.icd_10_cm) as diagnosis_code_1_normalized
    , med.diagnosis_code_2 as diagnosis_code_2_source
    , coalesce(diag_2_9.icd_9_cm, diag_2_10.icd_10_cm) as diagnosis_code_2_normalized
    , med.diagnosis_code_3 as diagnosis_code_3_source
    , coalesce(diag_3_9.icd_9_cm, diag_3_10.icd_10_cm) as diagnosis_code_3_normalized
    , data_source
  from clean_header med
  left join wellbe.terminology.icd_10_cm as diag_1_10
    on med.diagnosis_code_1 = diag_1_10.icd_10_cm
    and med.diagnosis_code_type = 'icd-10-cm'
  left join wellbe.terminology.icd_9_cm as diag_1_9
    on med.diagnosis_code_1 = diag_1_9.icd_9_cm
    and med.diagnosis_code_type = 'icd-9-cm'
  left join wellbe.terminology.icd_10_cm as diag_2_10
    on med.diagnosis_code_2 = diag_2_10.icd_10_cm
    and med.diagnosis_code_type = 'icd-10-cm'
  left join wellbe.terminology.icd_9_cm as diag_2_9
    on med.diagnosis_code_2 = diag_2_9.icd_9_cm
    and med.diagnosis_code_type = 'icd-9-cm'
  left join wellbe.terminology.icd_10_cm as diag_3_10
    on med.diagnosis_code_3 = diag_3_10.icd_10_cm
    and med.diagnosis_code_type = 'icd-10-cm'
  left join wellbe.terminology.icd_9_cm as diag_3_9
    on med.diagnosis_code_3 = diag_3_9.icd_9_cm
    and med.diagnosis_code_type = 'icd-9-cm'