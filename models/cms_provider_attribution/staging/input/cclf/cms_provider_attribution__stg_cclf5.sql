select distinct
      cur_clm_uniq_id as claim_id
    , clm_line_num as claim_line_number
    , bene_mbi_id as person_id
    , clm_prvdr_spclty_cd 
    , clm_prcsg_ind_cd
    , clm_carr_pmt_dnl_cd
from {{ source('phds_lakehouse_test', 'yak_cclf_5')}}