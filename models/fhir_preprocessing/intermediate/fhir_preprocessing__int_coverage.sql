{{ config(
     enabled = var('fhir_preprocessing_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with base as (

    select
          person_id as patient_internal_id
        /* create hash due to FHIR limit of 64 characters for max length of strings */
        , {{ dbt_utils.generate_surrogate_key(['eligibility_id']) }} as resource_internal_id
        , payer as organization_name
        , plan as coverage_plan
        , payer_type
        , enrollment_start_date as coverage_period_start
        , enrollment_end_date as coverage_period_end
        , coalesce(subscriber_relation,'self') as coverage_relationship
        , 'active' as coverage_status
        , coalesce(subscriber_id,member_id) as coverage_subscriber_id
        , data_source
    from {{ ref('fhir_preprocessing__stg_core__eligibility') }}

)

/* Add HEDIS-required product type mapping
    PPO	Commercial PPO (preferred provider organization policy)
    POS	Commerical POS (point of service policy)
    CEP	Commercial EPO (exclusive provider organization)
    HMO	Commercial HMO (health maintenance organization policy)
    MCR	Medicare Advantage HMO (health maintenance organization policy)
    MP	Medicare Advantage PPO (preferred provider organization policy)
    MC	Medicare Cost
    SN1	Special Needs Plan—Chronic Condition
    SN2	Special Needs Plan—Institutionalized
    SN3	Special Needs Plan—Dual Eligible
    MCS	Medicare Advantage POS (point of service policy)
    MMP	Medicare-Medicaid Plans
    MDE	Medicare Direct Entities
    MD	Medicaid Disabled HMO
    MLI	Medicaid Low Income HMO
    MRB	Medical Review Board
    MCD	Medicaid
    MMO	Exchange HMO (health maintenance organization policy)
    MOS	Exchange POS (point of service policy)
    MPO	Exchange PPO (preferred provider organization policy)
    MEP	Exchange EPO (exclusive provider organization)
*/
, add_product as (

    select
          patient_internal_id
        , resource_internal_id
        , organization_name
        , coverage_plan
        , payer_type
        , coverage_period_start
        , coverage_period_end
        , coverage_relationship
        , coverage_status
        , coverage_subscriber_id
        , data_source
        , case
            when lower(payer_type) like '%commercial%' then 'PPO'
            when lower(payer_type) like '%self%' then 'PPO'
            when lower(payer_type) like '%medicare%' then 'MCR'
            when lower(payer_type) like '%medicaid%' then 'MCD'
            when lower(coverage_plan) like '%pos&' then 'POS'
            when lower(coverage_plan) like '%cep%' then 'CEP'
            when lower(coverage_plan) like '%hmo%' then 'HMO'
            when lower(coverage_plan) like '%MP%' then 'MP'
            when lower(coverage_plan) like '%MC%' then 'MC'
            when lower(coverage_plan) like '%SN1%' then 'SN1'
            when lower(coverage_plan) like '%SN2%' then 'SN2'
            when lower(coverage_plan) like '%SN3%' then 'SN3'
            when lower(coverage_plan) like '%MCS%' then 'MCS'
            when lower(coverage_plan) like '%MMP%' then 'MMP'
            when lower(coverage_plan) like '%MDE%' then 'MDE'
            when lower(coverage_plan) like '%MD%' then 'MD'
            when lower(coverage_plan) like '%MLI%' then 'MLI'
            when lower(coverage_plan) like '%MRB%' then 'MRB'
            when lower(coverage_plan) like '%MMO%' then 'MMO'
            when lower(coverage_plan) like '%MOS%' then 'MOS'
            when lower(coverage_plan) like '%MPO%' then 'MPO'
            when lower(coverage_plan) like '%MEP%' then 'MEP'
          end as coverage_type_product
    from base

)

select
      patient_internal_id
    , resource_internal_id
    , organization_name
    , coverage_plan
    , coverage_period_start
    , coverage_period_end
    , coverage_relationship
    , coverage_status
    , coverage_subscriber_id
    , data_source
    , coverage_type_product
from add_product
