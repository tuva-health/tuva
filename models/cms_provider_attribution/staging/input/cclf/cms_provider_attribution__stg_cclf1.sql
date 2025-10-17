
with base as (
select 
  {% for column in adapter.get_columns_in_relation(source('phds_lakehouse_test', 'yak_cclf_1')) %}
    {% if column.name != 'ingest_datetime' %}
      cast(nullif(trim({{ column.name }}), '') as {{ dbt.type_string() }}) as {{ column.name }}
    {% else %}
      {{ column.name }}
    {% endif %}
    {%- if not loop.last -%},{%- endif %}
  {% endfor %}
from {{ source('phds_lakehouse_test', 'yak_cclf_1') }}
)

, cclf1 as (
select 
      CONCAT('A',
      SUBSTRING(filename, CHARINDEX('P.A', filename) + 3, 4
                )) as aco_id
    , cast(cur_clm_uniq_id as {{ dbt.type_string() }})  as claim_id
    , cast(prvdr_oscar_num as {{ dbt.type_string() }}) as ccn
    , cast(bene_mbi_id as {{ dbt.type_string() }}) as person_id
    , cast(bene_hic_num as {{ dbt.type_string() }}) as bene_hic_num
    , cast(clm_type_cd as {{ dbt.type_string() }}) as clm_type_cd
    , cast(clm_from_dt as {{ dbt.type_string() }}) as clm_from_dt
    , cast(clm_thru_dt as {{ dbt.type_string() }}) as clm_thru_dt
    , cast(clm_bill_fac_type_cd as {{ dbt.type_string() }}) as clm_bill_fac_type_cd
    , cast(clm_bill_clsfctn_cd as {{ dbt.type_string() }}) as clm_bill_clsfctn_cd
    , cast(prncpl_dgns_cd as {{ dbt.type_string() }}) as prncpl_dgns_cd
    , cast(admtg_dgns_cd as {{ dbt.type_string() }}) as admtg_dgns_cd
    , cast(clm_mdcr_npmt_rsn_cd as {{ dbt.type_string() }}) as clm_mdcr_npmt_rsn_cd
    , cast(clm_pmt_amt as {{ dbt.type_string() }}) as clm_pmt_amt
    , cast(clm_nch_prmry_pyr_cd as {{ dbt.type_string() }}) as clm_nch_prmry_pyr_cd
    , cast(prvdr_fac_fips_st_cd as {{ dbt.type_string() }}) as prvdr_fac_fips_st_cd
    , cast(bene_ptnt_stus_cd as {{ dbt.type_string() }}) as bene_ptnt_stus_cd
    , cast(dgns_drg_cd as {{ dbt.type_string() }}) as dgns_drg_cd
    , cast(clm_op_srvc_type_cd as {{ dbt.type_string() }}) as clm_op_srvc_type_cd
    , cast(fac_prvdr_npi_num as {{ dbt.type_string() }}) as fac_prvdr_npi_num
    , cast(oprtg_prvdr_npi_num as {{ dbt.type_string() }}) as oprtg_prvdr_npi_num
    , cast(atndg_prvdr_npi_num as {{ dbt.type_string() }}) as atndg_prvdr_npi_num
    , cast(othr_prvdr_npi_num as {{ dbt.type_string() }}) as othr_prvdr_npi_num
    , cast(clm_adjsmt_type_cd as {{ dbt.type_string() }}) as clm_adjsmt_type_cd
    , cast(clm_efctv_dt as {{ dbt.type_string() }}) as clm_efctv_dt
    , cast(clm_idr_ld_dt as {{ dbt.type_string() }}) as clm_idr_ld_dt
    , cast(bene_eqtbl_bic_hicn_num as {{ dbt.type_string() }}) as bene_eqtbl_bic_hicn_num
    , cast(clm_admsn_type_cd as {{ dbt.type_string() }}) as clm_admsn_type_cd
    , cast(clm_admsn_src_cd as {{ dbt.type_string() }}) as clm_admsn_src_cd
    , cast(clm_bill_freq_cd as {{ dbt.type_string() }}) as clm_bill_freq_cd
    , cast(clm_query_cd as {{ dbt.type_string() }}) as clm_query_cd
    , cast(dgns_prcdr_icd_ind as {{ dbt.type_string() }}) as dgns_prcdr_icd_ind
    , cast(clm_mdcr_instnl_tot_chrg_amt as {{ dbt.type_string() }}) as clm_mdrc_instnl_tot_chrg_amt
    , cast(clm_mdcr_ip_pps_cptl_ime_amt as {{ dbt.type_string() }}) as clm_mdcr_ip_pps_cptl_ime_amt
    , cast(clm_oprtnl_ime_amt as {{ dbt.type_string() }}) as clm_oprtnl_ime_amt
    , cast(clm_mdcr_ip_pps_dsprprtnt_amt as {{ dbt.type_string() }}) as clm_mdr_ip_pps_dsprprtnt_amt
    , cast(clm_hipps_uncompd_care_amt as {{ dbt.type_string() }}) as clm_hipps_uncompd_care_amt
    , cast(clm_oprtnl_dsprprtnt_amt as {{ dbt.type_string() }}) as clm_oprtnl_dsprprtnt_amt
    , cast(clm_blg_prvdr_oscar_num as {{ dbt.type_string() }}) as clm_blg_prvdr_oscar_num
    , cast(clm_blg_prvdr_npi_num as {{ dbt.type_string() }}) as clm_blg_prvdr_npi_num
    , cast(clm_oprtg_prvdr_npi_num as {{ dbt.type_string() }}) as clm_oprtg_prvdr_npi_num
    , cast(clm_atndg_prvdr_npi_num as {{ dbt.type_string() }}) as clm_atndg_prvdr_npi_num
    , cast(clm_othr_prvdr_npi_num as {{ dbt.type_string() }}) as clm_othr_prvdr_npi_num
    , cast(clm_cntl_num as {{ dbt.type_string() }}) as clm_cntl_num
    , cast(clm_org_cntl_num as {{ dbt.type_string() }}) as clm_org_cntl_num
    , cast(clm_cntrctr_num as {{ dbt.type_string() }}) as clm_cntrctr_num
    , cast(filename as {{ dbt.type_string() }}) as file_name
    , cast(
            TRY_CONVERT(date, 
                SUBSTRING(filename, 
                    CHARINDEX('.D', filename) + 2, 6
                ), 
                12
            ) 
        AS date) AS file_date
    , cast(ingest_datetime as {{ dbt.type_timestamp() }}) as ingest_datetime
from base
)

, extract_performance_year as (
select 
  cclf1.*
  , SUBSTRING(file_name, 
      CHARINDEX('.D', file_name) - 3, 3
    ) as performance_year_base
from cclf1
)

, add_performance_year as (
select
    cclf1.* 
  , 2000 + substring(performance_year_base,2,2) as performance_year 
  , case when upper(performance_year_base) like 'R%' then 1 else 0 end as runout_file
from extract_performance_year cclf1
)

, ranking as (
select 
      person_id
    , claim_id
    , clm_type_cd
    , nullif(othr_prvdr_npi_num,'~') as othr_prvdr_npi_num
    , nullif(atndg_prvdr_npi_num,'~') as atndg_prvdr_npi_num
    , nullif(oprtg_prvdr_npi_num,'~') as oprtg_prvdr_npi_num
    , ccn
    , clm_mdcr_npmt_rsn_cd
    , rank() over (partition by claim_id order by file_date desc) as desc_date_rank
from add_performance_year
)

select 
    person_id
  , claim_id
  , clm_type_cd
  , othr_prvdr_npi_num
  , atndg_prvdr_npi_num
  , oprtg_prvdr_npi_num
  , ccn
  , clm_mdcr_npmt_rsn_cd
from ranking
where desc_date_rank = 1