
select * from {{ ref('core__stg_claims_location') }}
union all
select * from {{ ref('core__stg_clinical_location') }}
