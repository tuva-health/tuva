


-- This dbt model has these columns:
--     claim_id
--     revenue_center_code
--     valid_rev_code (0 or 1 flag, indicates if the code is from terminology)
--     revenue_center_description
--     basic (0 or 1 flag, indicates if the rev code is a 'basic' code)
--     hospice (0 or 1 flag, indicates if the rev code is a 'hospice' code)
--     loa (0 or 1 flag, indicates if the rev code is a 'leave of absence' code)
--     behavioral (0 or 1 flag, indicates if the rev code is a 'behavioral' code)

-- This model has one row per rev code present in the input layer 'medical_claim'
-- table that is between '0100' and '0219' or between '1000' and '1002'
-- (those are the ranges of rev codes that are considered 'Room & Board' codes,
--  but note that there may be codes in that range that are not in terminology,
--  so they are therefore invalid codes, which is why we have a valid_rev_code
--  flag)

-- Note that we may multiple room and board rev codes per claim_id.




with all_room_and_board_rev_codes as (
select 
  aa.claim_id,
  aa.revenue_center_code,

  case
    when bb.revenue_center_code is not null then 1
    else 0
  end as valid_rev_code,

  bb.revenue_center_description,


  case
    when 
(aa.revenue_center_code in
  ('0100','0101',
   '0110','0111','0112','0113','0114','0116','0117','0118','0119', -- removed '0115' (Hospice)
   '0120','0121','0122','0123','0124','0126','0127','0128','0129', -- removed '0125' (Hospice)
   '0130','0131','0132','0133','0134','0136','0137','0138','0139', -- removed '0135' (Hospice)
   '0140','0141','0142','0143','0144','0146','0147','0148','0149', -- removed '0145' (Hospice)
   '0150','0151','0152','0153','0154','0156','0157','0158','0159', -- removed '0155' (Hospice)
   '0160','0164','0167','0169',
   '0170','0171','0172','0173','0174','0179',
                                         -- removed '0180' (Leave of Absense)
					 -- removed '0182' (Patient Convenience)
					 -- removed '0183' (Therapeutic Leave)
					 -- removed '0185' (Nursing home for hospitalization)
					 -- removed '0189' (Other leave of absense)
   '0190','0191','0192','0193','0194','0199',
   '0200','0201','0202','0203','0204','0206','0207','0208','0209',
   '0210','0211','0212','0213','0214','0219')
)   then 1
    else 0
  end as basic,



  case
    when
(aa.revenue_center_code in ('0115','0125','0135','0145','0155') ) then 1
    else 0
  end as hospice,


  
  case
    when
(aa.revenue_center_code in ('0180','0182','0183','0185','0189') ) then 1
    else 0
  end as loa,



  case
    when 
(aa.revenue_center_code in ('1000','1001','1002') ) then 1
    else 0
  end as behavioral


from {{ ref('core__medical_claim') }} aa

left join {{ ref('terminology__revenue_center') }} bb
on aa.revenue_center_code = bb.revenue_center_code

where
(aa.revenue_center_code between '0100' and '0219')
or
(aa.revenue_center_code between '1000' and '1002')
)


select *
from all_room_and_board_rev_codes
