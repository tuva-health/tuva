{{ config(
     enabled = var('claims_preprocessing_enabled',var('tuva_marts_enabled',True))
   )
}}

-- *************************************************
-- This dbt model assigns service categories to
-- every claim line in the medical_claim table.
-- It returns a table with these 4 columns:
--      claim_id
--      claim_line_number
--      service_category_1
--      service_category_2
-- The number of rows in this table should be equal to
-- the number of rows in the medical_claim table.
-- *************************************************




with room_and_board_rev_code_claims as (
select distinct claim_id
from {{ ref('input_layer__medical_claim') }}
where revenue_center_code in
  ('0100','0101',
   '0110','0111','0112','0113','0114','0116','0117','0118','0119',
   '0120','0121','0122','0123','0124','0126','0127','0128','0129',
   '0130','0131','0132','0133','0134','0136','0137','0138','0139',
   '0140','0141','0142','0143','0144','0146','0147','0148','0149',
   '0150','0151','0152','0153','0154','0156','0157','0158','0159',
   '0160','0164','0167','0169',
   '0170','0171','0172','0173','0174','0179',
   '0190','0191','0192','0193','0194','0199',
   '0200','0201','0202','0203','0204','0206','0207','0208','0209',
   '0210','0211','0212','0213','0214','0219',
   '1000','1001','1002')
),


valid_drg_claims as (
select distinct claim_id
from {{ ref('input_layer__medical_claim') }} mc

left join {{ ref('terminology__ms_drg')}} msdrg
on mc.ms_drg_code = msdrg.ms_drg_code
where (msdrg.ms_drg_code is not null)
-- need to join to APR DRG too!!!!!!
),


ed_rev_code_claims as (
select distinct claim_id
from {{ ref('input_layer__medical_claim') }}
where revenue_center_code in ('0450','0451','0452','0459','0981')
-- 0456, urgent care, is included in most published definitions
-- that also include a requirement of a bill type code for
-- inpatient or outpatient hospital.
),


urgent_care_rev_code_claims as (
select distinct claim_id
from {{ ref('input_layer__medical_claim') }}
where revenue_center_code = '0456'
),


grouping_cte as
(
---- INSTITUTIONAL ------
select distinct
    claim_id,
    claim_line_number,


    case
      when ( (hcpcs_code between 'E0100' and 'E8002') or
             (hcpcs_code between 'A0425' and 'A0436') or
	     left(bill_type_code,2) in ('14')
	   ) then 'Ancillary' -- DME or Ambulance or Lab
      when left(bill_type_code,2) in
           ('11','12','21','22','82','41','42','51','61','62')
	   then 'Inpatient'
      when left(bill_type_code,2) in
           ('13','71','73','23','52','31','32','33',
	    '81','72','43','53','63','83') then 'Outpatient'
      else 'Other'
    end as service_category_1,


    case
      when (hcpcs_code between 'E0100' and 'E8002') then 'DME'
      when (hcpcs_code between 'A0425' and 'A0436') then 'Ambulance'
      when left(bill_type_code,2) in ('14') then 'Lab'
      when left(bill_type_code,2) in ('11','12') 
            and claim_id in
	        (select claim_id from room_and_board_rev_code_claims)
            and claim_id in (
	        select claim_id from valid_drg_claims)
            then 'Acute Inpatient' 
      when left(bill_type_code,2) in ('21','22') then 'Skilled Nursing'
      when left(bill_type_code,2) in ('82') then 'Hospice'
      when left(bill_type_code,2) in ('41','42','51','61','62') then 'Other'
      when left(bill_type_code,2) in ('13','71','73') then
        case 
          when claim_id in
	       (select claim_id from ed_rev_code_claims)
	       then 'Emergency Department'
          when claim_id in
	       (select claim_id from urgent_care_rev_code_claims)
	       then 'Urgent Care'
          else 'Outpatient Hospital/Clinic'
        end
      when left(bill_type_code,2) in ('52') then 'Outpatient Psychiatric'
      when left(bill_type_code,2) in ('31','32','33') then 'Home Health'
      when left(bill_type_code,2) in ('81') then 'Hospice'
      when left(bill_type_code,2) in ('72') then 'Dialysis'
      else 'Other' 
    end as service_category_2
	
from {{ ref('input_layer__medical_claim') }}
where claim_type = 'institutional'

union all

---- PROFESSIONAL --------    
select
     claim_id
    ,claim_line_number
    ,case 
        when hcpcs_code between 'E0100' and 'E8002' or hcpcs_code between 'A0425' and 'A0436' or place_of_service_code in ('81','41','42') then 'Ancillary' -- DME or Ambulance or Lab
        when place_of_service_code in ('21','31','32','34','13','14','33','54','51','55','56','61') then 'Inpatient' 
        when place_of_service_code in ('15','17','19','22','49','50','60','71','72','23','24','20','65','12','52','53','57','58','62') then 'Outpatient' 
        when place_of_service_code in ('11','02') then 'Office Visit' 
        else 'Other' end
        as service_category_1
    ,case
        when hcpcs_code between 'E0100' and 'E8002' then 'DME'
        when hcpcs_code between 'A0425' and 'A0436' or place_of_service_code in ('41','42') then 'Ambulance'
        when place_of_service_code in ('81') then 'Lab'
        when place_of_service_code in ('21') then 'Acute Inpatient'
        when place_of_service_code in ('31','32') then 'Skilled Nursing'
        when place_of_service_code in ('34') then 'Hospice'
        when place_of_service_code in ('51','55','56') then 'Inpatient Psychiatric'
        when place_of_service_code in ('61') then 'Inpatient Rehabilitation'
        when place_of_service_code in ('15','17','19','22','49','50','60','71','72') then 'Outpatient Hospital/Clinic'
        when place_of_service_code in ('23') then 'Emergency Department'
        when place_of_service_code in ('24') then 'Ambulatory Surgery'
        when place_of_service_code in ('20') then 'Urgent Care'
        when place_of_service_code in ('65') then 'Dialysis'
        when place_of_service_code in ('12') then 'Home Health'
        when place_of_service_code in ('52','53','57','58') then 'Outpatient Psychiatric'
        when place_of_service_code in ('62') then 'Outpatient Rehabilitation'
        when place_of_service_code in ('11','02') then 'Office Visit'
        else 'Other'
        end
        as service_category_2
from {{ ref('input_layer__medical_claim') }}
where claim_type = 'professional'
)


select *
from grouping_cte
