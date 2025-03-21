select
     cast(min(med.person_id) as varchar(50)) as [Person ID]
   , cast(concat(med.claim_id, med.data_source) as varchar(100)) as [Claim Header ID]
   , cast(null as varchar(100)) as [Adjustment to Claim ID]
   , cast(null as varchar(100)) as [Reversal to Claim ID]
   , 0 as [Adjustment Type Code]
   , 0 as [Adjustment Sequence]
   , min(case
         when med.claim_type = 'institutional' then 1
         when med.claim_type = 'professional' then 2
         when med.claim_type = 'oral' then 3
         when med.claim_type = 'vision' then 4
         when med.claim_type = 'pharmacy' then 5
         else null
     end) as [Claim Type Code]
   , cast(null as varchar(8)) as [Claim Received Date]
   , cast(null as varchar(8)) as [Claim Processed Date]
   , convert(varchar(8), max(med.paid_date), 112) as [Claim Paid Date]
   , cast(max(med.bill_type_code) as varchar(4)) as [Type of Bill Code]
   , convert(varchar(8), min(med.claim_line_start_date), 112) as [Service Start Date]
   , convert(varchar(8), max(med.claim_line_end_date), 112) as [Service End Date]
   , convert(varchar(8), min(med.admission_date), 112) as [Admit Date]
   , convert(varchar(8), max(med.discharge_date), 112) as [Discharge Date]
   , min(case 
         when cc.source_code_type = 'icd-10-cm' then 2
         when cc.source_code_type = 'icd-9-cm' then 1
         else null
     end) as [Primary Diagnosis Code Set]

   , cast(null as varchar(10)) as [Primary Diagnosis Code]
   , cast(null as int) as [Admitting Diagnosis Code Set]
   , cast(null as varchar(10)) as [Admitting Diagnosis Code]
   , min(case
         when med.drg_code_type = 'cms-drg' then 1
         when med.drg_code_type = 'ms-drg' then 2
         when med.drg_code_type = 'ap-drg' then 3
         when med.drg_code_type = 'apr-drg' then 4
         when med.drg_code_type = 'aps-drg' then 5
         when med.drg_code_type = 'tricare' then 6
         else null
     end) as [DRG Code Type]

   , cast(null as int) as [DRG Code Set]
   , cast(max(med.drg_code) as varchar(3)) as [DRG Code]
   , cast(null as int) as [Severity of Illness]
   , cast(null as int) as [Risk of Mortality]
   , cast(null as int) as [Primary Procedure Code Set]
   , cast(max(med.hcpcs_code) as varchar(10)) as [Primary Procedure Code]
   , cast(max(med.admit_type_code) as int) as [Admission Type Code]
   , cast(max(med.admit_source_code) as varchar(1)) as [Admission Source Code]
   , cast(max(prov_bil.provider_last_name) as varchar(100)) as [Billing Provider Last Name]
   , cast(max(prov_bil.provider_first_name) as varchar(100)) as [Billing Provider First Name]
   , cast(null as varchar(25)) as [Billing Provider Middle Name]
   , cast(max(prov_bil.practice_address_line_1) as varchar(100)) as [Billing Provider Address]
   , cast(max(prov_bil.practice_address_line_2) as varchar(25)) as [Billing Provider Address Line 2]
   , cast(max(prov_bil.practice_city) as varchar(100)) as [Billing Provider City]
   , cast(max(prov_bil.practice_state) as varchar(2)) as [Billing Provider State]
   , cast(max(prov_bil.practice_zip_code) as varchar(10)) as [Billing Provider ZIP Code]
   , cast(max(med.billing_id) as int) as [Billing Provider NPI]
   , cast(max(prov_bil.primary_taxonomy_code) as varchar(10)) as [Billing Provider NUCC Taxonomy Code]
   , 1 as [Billing Provider Specialty Code Set]
   , cast(max(prov_bil.primary_specialty_description) as varchar(10)) as [Billing Provider Specialty Code]
   , cast(null as int) as [Referring Provider NPI]
   , cast(null as varchar(10)) as [Referring Provider NUCC Taxonomy Code]
   , cast(null as int) as [Referring Provider Specialty Code Set]
   , cast(null as varchar(10)) as [Referring Provider Specialty Code]
   , cast(max(med.rendering_id) as int) as [Attending Provider NPI]
   , cast(max(prov_att.primary_taxonomy_code) as varchar(10)) as [Attending Provider NUCC Taxonomy Code]
   , 1 as [Attending Provider Specialty Code Set]
   , cast(max(prov_att.primary_specialty_description) as varchar(10)) as [Attending Provider Specialty Code]
   , cast(null as int) as [Operating Provider NPI]
   , cast(null as varchar(10)) as [Operating Provider NUCC Taxonomy Code]
   , cast(null as int) as [Operating Provider Specialty Code Set]
   , cast(null as varchar(10)) as [Operating Provider Specialty Code]
   , cast(null as varchar(10)) as [Other Operating Provider NPI]
   , cast(null as int) as [Admitting Provider NPI]
   , cast(null as varchar(10)) as [Admitting Provider NUCC Taxonomy Code]
   , cast(null as int) as [Admitting Provider Specialty Code Set]
   , cast(null as varchar(10)) as [Admitting Provider Specialty Code]
   , cast(max(med.place_of_service_code) as varchar(10)) as [Place of Service NPI]
   , cast(max(med.place_of_service_description) as varchar(100)) as [Place of Service Name]
   , cast(max(prov_pos.practice_address_line_1) as varchar(100)) as [Place of Service Address]
   , cast(max(prov_pos.practice_address_line_2) as varchar(55)) as [Place of Service Address Line 2]
   , cast(max(prov_pos.practice_city) as varchar(100)) as [Place of Service City]
   , cast(max(prov_pos.practice_state) as varchar(2)) as [Place of Service State]
   , cast(max(prov_pos.practice_zip_code) as varchar(10)) as [Place of Service ZIP Code]
   , cast(max(med.billing_tin) as varchar(10)) as [TIN]
   , cast(null as int) as [Filing Order]
   , sum(med.charge_amount) as [Total Billed Amount]
   , sum(med.allowed_amount) as [Total Allowed Amount]
   , sum(med.copayment_amount) as [Total Copay Amount]
   , sum(med.coinsurance_amount) as [Total Coinsurance Amount]
   , sum(med.deductible_amount) as [Total Deductible Amount]
   , cast(null as numeric) as [Total Not Covered Amount]
   , sum(med.paid_amount) as [Total Paid Amount]
   , cast(null as numeric) as [Total COB Amount]
   , cast(max(med.discharge_disposition_code) as varchar(3)) as [Discharge Disposition Code]
   , max(med.in_network_flag) as [Is In Network]
   , 1 as [Processing Status]
   , cast(min(med.claim_id) as varchar(50)) as [Payer Claim ID]
   , cast(min(c.contract_id) as varchar(50)) as [Contract ID]
   , cast(null as varchar(100)) as [Region Name]
   , cast(null as varchar(100)) as [Line of Business Name]
   , cast(null as varchar(100)) as [Corporation Name]
   , cast(null as varchar(100)) as [Group Name]
   , cast(null as varchar(100)) as [Benefit Plan Name]
   , cast(max(med.payer) as varchar(100)) as [Payer Name]
   , cast(max(med.[plan]) as varchar(50)) as [Plan Type]
   , cast(null as varchar(300)) as [Denial Reason]
from {{ ref('core__medical_claim') }} as med
inner join {{ ref('epic__contract_id') }} c
    on med.data_source = c.data_source
   and med.payer = c.payer
   and med.[plan] = c.[plan]
left join {{ ref('core__condition') }} cc
    on med.claim_id = cc.claim_id
   and cc.condition_rank = 1
left join {{ ref('terminology__provider') }} prov_bil
    on med.billing_id = prov_bil.npi
left join {{ ref('terminology__provider') }} prov_att
    on med.rendering_id = prov_att.npi
left join {{ ref('terminology__provider') }} prov_pos
    on med.facility_id = prov_pos.npi
group by
   cast(concat(med.claim_id, med.data_source) as varchar(100))
