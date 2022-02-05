select a.*
from {{ ref('stg_procedure') }} a
left join {{ ref('icd_10_pcs') }} b
    on a.procedure_code = b.icd_10_pcs
where a.code_type = 'icd-10-pcs'
	and b.icd_10_pcs is null

union

select a.*
from {{ ref('stg_procedure') }} a
left join {{ ref('hcpcs_level_2') }} b
    on a.procedure_code = b.code
where a.code_type = 'hcpcs level 2'
	and b.code is null