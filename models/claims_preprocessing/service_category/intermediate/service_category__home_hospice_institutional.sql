{{ config(
     enabled = var('claims_enabled', False) | as_bool
   )
}}

/*
  Home hospice: hospice delivered where the patient lives. CMS pays routine and
  continuous home care the same way whether the residence is a house, an
  assisted living facility or a nursing facility, so all four residence
  site-of-service codes belong here.
*/

with bill_type_and_revenue as (

    select distinct
        med.claim_id
      , med.data_source
    from {{ ref('service_category__stg_medical_claim') }} as med
    inner join {{ ref('service_category__stg_outpatient_institutional') }} as outpatient
      on med.claim_id = outpatient.claim_id
      and med.data_source = outpatient.data_source
    where substring(med.bill_type_code, 1, 2) in ('81')
      or med.revenue_center_code in ('0650', '0651', '0652', '0657', '0659')

)

, home_site_of_service as (

    /* Q5001 home or residence, Q5002 assisted living facility, Q5003 nursing
       long term care or non-skilled nursing facility, Q5004 skilled nursing
       facility. */
    select distinct
        med.claim_id
      , med.data_source
    from {{ ref('service_category__stg_medical_claim') }} as med
    where med.claim_type = 'institutional'
      and med.hcpcs_code in ('Q5001', 'Q5002', 'Q5003', 'Q5004')
      and not exists (
        select 1
        from {{ ref('service_category__home_health_institutional') }} as hhi
        where med.claim_id = hhi.claim_id
        and med.data_source = hhi.data_source
      )
      and not exists (
        /* a facility site-of-service code on the same claim outranks a
           residence one */
        select 1
        from {{ ref('service_category__stg_medical_claim') }} as fac
        where fac.claim_id = med.claim_id
        and fac.data_source = med.data_source
        and fac.claim_type = med.claim_type
        and fac.hcpcs_code in ('Q5005', 'Q5006', 'Q5007', 'Q5008', 'Q5009', 'Q5010')
      )

)

, unioned as (

    select claim_id, data_source from bill_type_and_revenue
    union all
    select claim_id, data_source from home_site_of_service

)

select distinct
    claim_id
  , data_source
  , 'outpatient' as service_category_1
  , 'home hospice' as service_category_2
  , 'home hospice' as service_category_3
  , '{{ this.name }}' as source_model_name
  , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from unioned
