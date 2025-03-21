select 
     cast(p.claim_id as varchar(100)) as [Claim Header ID]
   , 0 as [Adjustment Type Code]
   , 0 as [Adjustment Sequence]
   , convert(varchar(8), p.procedure_date, 112) as [Performed Date]
   , 5 as [Procedure Code Set]
   , cast(p.normalized_code as varchar(10)) as [Procedure Code]
   , cast(p.normalized_description as varchar(200)) as [Procedure Description]
   , cast(null as numeric) as [Procedure Rank]
from {{ ref('core__procedure') }} as p
where lower(p.normalized_code_type) = 'icd-10-pcs'
