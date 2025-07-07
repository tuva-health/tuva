with service_category__stg_medical_claim as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_medical_claim') }}
),
service_category__stg_office_based as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_office_based') }}
),
--Since HCPCS is a string, we need to exclude values that would fall in our comparison range for alphanumeric values
numeric_hcpcs as (
    select medical_claim_sk, hcpcs_code
    from service_category__stg_medical_claim as med
    {% if target.type in ('duckdb', 'databricks') %}
        where try_cast('hcpcs_code' as integer) is not null
    {% else %}
        where {{ safe_cast('hcpcs_code', 'int') }} is not null
    {% endif %}
)
select
    med.medical_claim_sk
    , 'office-based' as service_category_1
    , 'office-based surgery' as service_category_2
    , 'office-based surgery' as service_category_3
from numeric_hcpcs as med
    inner join service_category__stg_office_based as office
    on med.medical_claim_sk = office.medical_claim_sk
where
    (hcpcs_code between '10021' and '69999')
