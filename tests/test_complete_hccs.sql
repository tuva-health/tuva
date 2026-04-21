--- This test ensures all HCCs are still accounted for after determining the gap status
select distinct person_id, payer, collection_year, model_version, hcc_code 
from {{ ref('ra_ops__int_recapturable_hccs')}}
where eligible_bene_flag = 1 
    and hcc_type = 'coded'
    and filtered_by_hierarchy_flag = 0

MINUS 

select distinct person_id, payer, payment_year - 1, model_version, hcc_code 
-- Using the int gap status since hierarchies aren't applied yet
from {{ ref('ra_ops__int_gap_status')}}
where gap_status != 'open'