
select *
from {{ ref('readmissions__readmission_summary') }}
where index_admission_flag = 1
