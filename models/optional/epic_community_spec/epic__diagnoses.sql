select 
     cast(claim_id as varchar(100)) as [Claim Header ID]
   , cast(null as varchar(50)) as [Claim Service Line ID]
   , 0 as [Adjustment Type Code]
   , 0 as [Adjustment Sequence]
   , case
         when lower(normalized_code_type) = 'icd-9-cm' then 1
         when lower(normalized_code_type) = 'icd-10-cm' then 2
         else null
     end as [Diagnosis Code Set]
   , cast(normalized_code as varchar(10)) as [Diagnosis Code]
   , cast(condition_rank as int) as [Diagnosis Rank]
   , case
         when present_on_admit_code in ('Y', '1', 'Yes') then 1
         when present_on_admit_code in ('N', '0', 'No') then 0
         else null
     end as [Present on Admission]
from {{ ref('core__condition') }}
