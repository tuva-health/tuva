{{ config(
     enabled = var('claims_enabled', False) | as_bool
   )
}}

/*
  Facility hospice: hospice delivered in a facility at an inpatient level of
  care (general inpatient, inpatient respite), as opposed to the routine and
  continuous home care hospice provides wherever the patient lives.
*/

with bill_type_and_revenue as (

    select distinct
        s.claim_id
      , s.data_source
    from {{ ref('service_category__stg_medical_claim') }} as s
      inner join {{ ref('service_category__stg_inpatient_institutional') }} as a
      on s.claim_id = a.claim_id
      and s.data_source = a.data_source
    where substring(s.bill_type_code, 1, 2) in ('82')
      or s.revenue_center_code in ('0655', '0656', '0658', '0115', '0125', '0135', '0145', '0155', '0235')

)

, facility_site_of_service as (

    /* Q5005 inpatient hospital, Q5006 inpatient hospice facility, Q5007 long
       term care facility, Q5008 inpatient psychiatric facility, Q5009 place
       not otherwise specified, Q5010 hospice home care in a hospice facility. */
    select distinct
        med.claim_id
      , med.data_source
    from {{ ref('service_category__stg_medical_claim') }} as med
    where med.claim_type = 'institutional'
      and med.hcpcs_code in ('Q5005', 'Q5006', 'Q5007', 'Q5008', 'Q5009', 'Q5010')
      and not exists (
        /* a hospice site-of-service code on a home health claim describes the
           home health visit, it does not make the claim hospice */
        select 1
        from {{ ref('service_category__home_health_institutional') }} as hhi
        where med.claim_id = hhi.claim_id
        and med.data_source = hhi.data_source
      )

)

, unioned as (

    select claim_id, data_source from bill_type_and_revenue
    union all
    select claim_id, data_source from facility_site_of_service

)

select distinct
    claim_id
  , data_source
  , 'inpatient' as service_category_1
  , 'facility hospice' as service_category_2
  , 'facility hospice' as service_category_3
  , '{{ this.name }}' as source_model_name
  , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from unioned
