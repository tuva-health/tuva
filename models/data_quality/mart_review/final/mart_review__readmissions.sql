SELECT *
FROM {{ ref('readmissions__readmission_summary') }}
WHERE index_admission_flag = 1
