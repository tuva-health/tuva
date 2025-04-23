{{ config(
     enabled = var('fhir_preprocessing_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}
select
      person_id as patient_internal_id
    /* create hash due to FHIR limit of 64 characters for max length of strings */
    , {{ dbt_utils.generate_surrogate_key(['eligibility_id']) }} as resource_internal_id
    , payer as coverage_payor
    , plan as coverage_plan
    , enrollment_start_date as coverage_period_start
    , enrollment_end_date as coverage_period_end
    , coalesce(subscriber_relation,'self') as coverage_relationship
    , 'active' as coverage_status
    , coalesce(subscriber_id,member_id) as coverage_subscriber_id
    /* HEDIS-required payer type
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
    , case
        when lower(payer_type) like '%commercial%' then 'PPO'
        when lower(payer_type) like '%self%' then 'PPO'
        when lower(payer_type) like '%medicare%' then 'MCR'
        when lower(payer_type) like '%medicaid%' then 'MCD'
        when lower(plan) like '%pos&' then 'POS'
        when lower(plan) like '%cep%' then 'CEP'
        when lower(plan) like '%hmo%' then 'HMO'
        when lower(plan) like '%MP%' then 'MP'
        when lower(plan) like '%MC%' then 'MC'
        when lower(plan) like '%SN1%' then 'SN1'
        when lower(plan) like '%SN2%' then 'SN2'
        when lower(plan) like '%SN3%' then 'SN3'
        when lower(plan) like '%MCS%' then 'MCS'
        when lower(plan) like '%MMP%' then 'MMP'
        when lower(plan) like '%MDE%' then 'MDE'
        when lower(plan) like '%MD%' then 'MD'
        when lower(plan) like '%MLI%' then 'MLI'
        when lower(plan) like '%MRB%' then 'MRB'
        when lower(plan) like '%MMO%' then 'MMO'
        when lower(plan) like '%MOS%' then 'MOS'
        when lower(plan) like '%MPO%' then 'MPO'
        when lower(plan) like '%MEP%' then 'MEP'
      end as coverage_type
    , data_source
from {{ ref('fhir_preprocessing__stg_core__eligibility') }}
