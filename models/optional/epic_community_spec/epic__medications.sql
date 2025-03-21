select
     cast(pharm.claim_id as varchar(100)) as [Claim Header ID]
   , cast(pharm.claim_line_number as varchar(50)) as [Claim Medication Line ID]
   , 0 as [Adjustment Type Code]
   , 0 as [Adjustment Sequence]
   , try_cast(pharm.prescribing_provider_id as bigint) as [Prescribing Provider NPI]
   , cast(prescriber.primary_taxonomy_code as varchar(10)) as [Prescribing Provider NUCC Taxonomy Code]
   , 1 as [Prescribing Provider Specialty Code Set]
   , cast(prescriber.primary_taxonomy_code as varchar(10)) as [Prescribing Provider Specialty Code]
   , try_cast(pharm.dispensing_provider_id as bigint) as [Service Provider NPI]
   , cast(dispensing.primary_taxonomy_code as varchar(10)) as [Service Provider NUCC Taxonomy Code]
   , 1 as [Service Provider Specialty Code Set]
   , cast(dispensing.primary_taxonomy_code as varchar(10)) as [Service Provider Specialty Code]
   , convert(varchar(8), pharm.dispensing_date, 112) as [Date Filled]
   , cast(pharm.ndc_code as varchar(11)) as [NDC]
   , cast(null as varchar(1)) as [DAW Code]
   , 1 as [Fill Status]
   , cast(pharm.quantity as numeric) as [Quantity Dispensed]
   , cast(pharm.days_supply as numeric) as [Days Supply]
   , cast(pharm.refills as numeric) as [Fill Number]
   , cast(pharm.charge_amount as numeric) as [Billed Amount]
   , cast(pharm.allowed_amount as numeric) as [Allowed Amount]
   , cast(pharm.copayment_amount as numeric) as [Copay Amount]
   , cast(pharm.coinsurance_amount as numeric) as [Coinsurance Amount]
   , cast(pharm.deductible_amount as numeric) as [Deductible Amount]
   , cast((pharm.charge_amount - coalesce(pharm.allowed_amount, 0)) as numeric) as [Not Covered Amount]
   , cast(pharm.paid_amount as numeric) as [Paid Amount]
from {{ ref('core__pharmacy_claim') }} as pharm
left join {{ ref('terminology__provider') }} prescriber
    on pharm.prescribing_provider_id = prescriber.npi
left join {{ ref('terminology__provider') }} dispensing
    on pharm.dispensing_provider_id = dispensing.npi
