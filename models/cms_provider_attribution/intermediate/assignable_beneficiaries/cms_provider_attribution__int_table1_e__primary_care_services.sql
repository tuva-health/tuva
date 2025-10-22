
/**** Primary Care Physicians: Section 2.1, Table 1, E ****

"Beneficiary had at least one primary care service with a
physician who is an ACO professional in the ACO, and is a
primary care physician (defined in § 425.20) or has a primary
specialty designation specified in § 425.402(c) (listed in
Appendix C, Table 11)."
*/

/* APPENDIX D: OUTPATIENT FACILITY CLAIMS USED IN BENEFICIARY ASSIGNMENT

p. 95 of the SLAAM

"Beneficiary assignment includes services provided in FQHCs, RHCs, Method II CAHs, and ETA
hospitals. The claims data used for assignment for these four provider types are limited to
outpatient facility claims. As described in this appendix, additional steps are used to identify
data on outpatient facility claims."

p. 90 of the SLAAM
Table 11 lists specialty codes used to identify physicians who are the basis for beneficiary
assignment. Physician specialty is identified by the specialty code associated with each line
item on a claim. The table includes the specialty codes used to identify primary care physicians
and physicians with other specialty designations, for purposes of identifying primary care
services furnished to beneficiaries used in assignment operations.
*/

-- Remove coverage month
with distinct_benes as (
select distinct 
      performance_year
    , aco_id
    , person_id
from {{ref('cms_provider_attribution__stg_beneficiary_demographics')}} 
)

-- Identify SNF and inpatient claims overlap
, claims_snf_or_inpatient as (
select distinct
      person_id
    , claim_start_date
    , claim_end_date
    , claim_id
    , case 
        -- TODO: Confirm if there is any official CMS guidance on specific codes
        when substring(bill_type_code,1,2) in ('21','22','23','28') then 1
        when place_of_service_code in (31,32) then 1
        else 0
      end as snf_facility_claim
    , case
        -- TODO: Confirm if there is any official CMS guidance on specific codes
        -- Could be any of these codes as well: place_of_service_code in (21,31,51,61)
        when substring(bill_type_code,1,2) in ('11','12','21','22') then 1
        when place_of_service_code = 21 then 1
        else 0
      end as inpatient_facility_claim
from {{ref('input_layer__medical_claim')}} med
inner join {{ref('cms_provider_attribution__stg_primary_care_hcpcs_codes')}} pc_codes
    on med.hcpcs_code = pc_codes.hcpcs_code    
)

/* p.92 of the 2025 SLAAM says, 
"According to § 425.400(c)(1)(v)–(ix), for the performance year starting on January 1, 2021, and
subsequent performance years, professional services or services reported on an FQHC or RHC claim
identified by CPT codes 99304–99318 are excluded for purposes of assigning beneficiaries when
furnished in a SNF."

Bill type codes reference found here: https://www.cms.gov/Regulations-and-Guidance/Guidance/Transmittals/downloads/R1775CP.pdf
Place of service codes here: https://resdac.org/cms-data/variables/line-place-service-code-ffs
*/
, snf_excluded_claims as (
select distinct    
      med.person_id
    , med.claim_id
    , med.claim_line_number
from {{ref('input_layer__medical_claim')}} med
inner join claims_snf_or_inpatient snf
    on  med.person_id = snf.person_id
    and (med.claim_start_date between snf.claim_start_date and snf.claim_end_date
            or med.claim_end_date between snf.claim_start_date and snf.claim_end_date
    )
    and snf_facility_claim = 1
)

/* p.92 of the 2025 SLAAM says, 
"According to § 425.400(c)(1)(v)–(ix), for the performance year starting on January 1, 2021, and
subsequent performance years, CMS excludes from use in the assignment methodology advance care
planning services claims billed under CPT codes 99497 and 99498 when such services identified by
these codes are furnished in an inpatient care setting."

Bill type codes reference found here: https://www.cms.gov/Regulations-and-Guidance/Guidance/Transmittals/downloads/R1775CP.pdf
Place of service codes here: https://resdac.org/cms-data/variables/line-place-service-code-ffs
*/
, inpatient_excluded_claims as (
select distinct    
      med.person_id
    , med.claim_id
    , med.claim_line_number
from {{ref('input_layer__medical_claim')}} med
inner join claims_snf_or_inpatient ip
    on  med.person_id = ip.person_id
    and (med.claim_start_date between ip.claim_start_date and ip.claim_end_date
            or med.claim_end_date between ip.claim_start_date and ip.claim_end_date
    )
    and inpatient_facility_claim = 1
    and med.claim_type = 'institutional'
)


-- Table 13. p. 97: Filter to outpatient primary care claims
, outpatient_primary_care_claims as (
select
    med.*
    , case 
        when ((substring(bill_type_code,1,2) = '77' and claim_start_date >= '2010-04-01') or
              (substring(bill_type_code,1,2) = '73' and claim_start_date < '2010-04-01')) 
              then 'FQHC'
        when substring(bill_type_code,1,2) = '71' then 'RHC'
        -- TODO: Allowed charges for ETA are imputed , see p.96 of the 2025 SLAAM
        when substring(bill_type_code,1,2) = '13' and claim_type_code = '40' then 'ETA'
        when substring(bill_type_code,1,2) = '85' and (
                substring(revenue_center_code,1,3) = '096' or
                substring(revenue_center_code,1,3) = '097' or
                substring(revenue_center_code,1,3) = '098')
            then 'Method II CAH'
        end as outpatient_facility
from {{ref('input_layer__medical_claim')}} as med

)


-- Filter ETA and Method II CAH based on providers
, outpatient_primary_care_benes as (
select 
      benes.*  
    , outpt.claim_id
    , outpt.claim_line_number
    , outpt.claim_start_date
    , outpt.paid_date
    , outpt.file_date
    , outpt.hcpcs_code
    -- Using paid amount here instead since CCLF2 has no allowed amount field
    , outpt.paid_amount as allowed_amount
    -- ETA and Method II CAH will be joined in via NPI, which is why the outpatient != 0 when they are identified
    , case 
        -- A tilde was being used as a stand-in for nulls
        when outpatient_facility = 'ETA' then coalesce(other_npi,attending_npi)
        when outpatient_facility = 'Method II CAH' then coalesce(rendering_npi, operating_npi, other_npi, attending_npi)
        else outpt.rendering_npi
      end as npi
    , null as tin
    , outpt.ccn
    , case when outpatient_facility in ('FQHC', 'RHC') then 1 else 0 end as fqhc_rhc_flag
    , case when outpatient_facility = 'Method II CAH' then 1 else 0 end as method_ii_cah_flag
    , case when outpatient_facility = 'ETA' then 1 else 0 end as eta_flag
from distinct_benes as benes
-- TODO: Add specific SNF hcpcs codes exclusions
inner join outpatient_primary_care_claims as outpt
    on  benes.person_id = outpt.person_id
    -- TODO: Add test to catch if data source is incorrect
    and (lower(outpt.data_source) like '%cclf%' or lower(outpt.data_source) like '%bcda%')
inner join {{ref('cms_provider_attribution__stg_primary_care_hcpcs_codes')}} pc_codes
    on outpt.hcpcs_code = pc_codes.hcpcs_code    
left join snf_excluded_claims as snf
    on  outpt.person_id = snf.person_id
    and outpt.claim_id = snf.claim_id
    and outpt.claim_line_number = snf.claim_line_number
    and outpt.hcpcs_code in ('99304','99305','99306','99307','99308','99309','99310','99315','99316','99318')
    and outpatient_facility in ('FQHC', 'RHC')
left join inpatient_excluded_claims as ip
    on  outpt.person_id = ip.person_id
    and outpt.claim_id = ip.claim_id
    and outpt.claim_line_number = ip.claim_line_number
    and outpt.hcpcs_code in ('99497', '99498')
left join {{ref('cms_provider_attribution__stg_electing_teaching_hospital_list')}} as eta
    on eta.ccn = outpt.ccn
    and eta.collection_year = benes.performance_year - 1
where 1=1
    and outpatient_facility is not null
    and 1 = (case 
                when outpatient_facility = 'ETA' and eta.ccn is null then 0
                else 1
              end 
            )
    -- p.90 in the SLAAM
    and 1 = (case 
                when outpt.hcpcs_code = 'G0463' and outpatient_facility = 'ETA' then 1 
                when outpt.hcpcs_code = 'G0463' then 0
                else 1
            end)
{% if var('performance_year') >= 2021 %}
    -- exclude claims furnished in a SNF or inpatient setting for applicable HCPCs codes
    and snf.claim_id is null
    and ip.claim_id is null
{% endif %}    
    
)

, non_outpatient_primary_care_benes as (
select
      benes.*
    , med.claim_id
    , med.claim_line_number
    , med.claim_start_date
    , med.paid_date
    , med.file_date
    , med.hcpcs_code
    , med.allowed_amount    
    , med.rendering_npi as npi
    , med.rendering_tin as tin
    , null as ccn
    , med.claim_provider_specialty_code
from distinct_benes as benes
inner join  {{ref('input_layer__medical_claim')}} as med
    on  benes.person_id = med.person_id
    and (lower(med.data_source) like '%cclf%' or lower(med.data_source) like '%bcda%')
inner join {{ref('cms_provider_attribution__stg_primary_care_hcpcs_codes')}} pc_codes
    on med.hcpcs_code = pc_codes.hcpcs_code   
    and pc_codes.hcpcs_code != 'G0463' -- Only for ETA claims 
left join snf_excluded_claims as snf
    on  med.person_id = snf.person_id
    and med.claim_id = snf.claim_id
    and med.claim_line_number = snf.claim_line_number
    and med.hcpcs_code in ('99304','99305','99306','99307','99308','99309','99310','99315','99316','99318')
    and claim_type = 'professional'
left join inpatient_excluded_claims as ip
    on  med.person_id = ip.person_id
    and med.claim_id = ip.claim_id
    and med.claim_line_number = ip.claim_line_number
    and med.hcpcs_code in ('99497', '99498')
where 1=1
{% if var('performance_year') >= 2021 %}
    -- exclude claims furnished in a SNF or inpatient setting for applicable HCPCs codes
    and snf.claim_id is null
    and ip.claim_id is null
{% endif %}
)

select distinct
      benes.performance_year
    , benes.aco_id
    , benes.person_id
    , benes.claim_id
    , benes.claim_line_number
    , benes.claim_start_date
    , benes.paid_date
    , benes.file_date    
    , benes.hcpcs_code    
    , benes.npi
    , benes.tin
    , benes.ccn
    , null as claim_provider_specialty_code
    , benes.allowed_amount
    , fqhc_rhc_flag
    , method_ii_cah_flag
    , eta_flag
    , 1 as outpatient_flag
from outpatient_primary_care_benes as benes

union all 

select 
      benes.performance_year
    , benes.aco_id
    , benes.person_id
    , benes.claim_id
    , benes.claim_line_number
    , benes.claim_start_date
    , benes.paid_date
    , benes.file_date
    , benes.hcpcs_code    
    , benes.npi
    , benes.tin
    , benes.ccn
    , benes.claim_provider_specialty_code
    , benes.allowed_amount
    , 0 as fqhc_rhc_flag
    , 0 as method_ii_cah_flag
    , 0 as eta_flag
    , 0 as outpatient_flag
from non_outpatient_primary_care_benes as benes
left join outpatient_primary_care_benes as otpt
    on  benes.performance_year = otpt.performance_year
    and benes.aco_id = otpt.aco_id
    and benes.person_id = otpt.person_id
    and benes.claim_id = otpt.claim_id
    and benes.claim_line_number = otpt.claim_line_number
where otpt.person_id is null -- Exclude outpatient since they are included in union above