with npi as (
    select *
    from {{ ref('the_tuva_project', 'core__stg_npi') }}
),
normalized_input__medical_claim as (
    select *
    from {{ ref('the_tuva_project', 'normalized_input__medical_claim') }}
),
service_category__medical_claim_service_category as (
    select *
    from {{ ref('the_tuva_project', 'service_category__medical_claim_service_category') }}
),
enrollment__medical_claim_member_month as (
    select *
    from {{ ref('the_tuva_project', 'enrollment__medical_claim_member_month') }}
),
encounters__int_crosswalk__claim_encounter as (
    select *
    from {{ ref('the_tuva_project', 'encounters__crosswalk__claim_encounter') }}
)
select
    med.medical_claim_sk
    , med.data_source
    , med.claim_id
    , med.claim_line_number
    , med.claim_type
    , member_month.patient_sk
    , med.person_id
    , med.member_id
    , med.payer
    , med.{{ quote_column('plan') }}
    , med.claim_start_date
    , med.claim_end_date
    , med.claim_line_start_date
    , med.claim_line_end_date
    , med.admission_date
    , med.discharge_date
    , med.admit_source_code
    , ref_admit_source.admit_source_description
    , med.admit_type_code
    , ref_admit_type.admit_type_description
    , med.discharge_disposition_code
    , ref_discharge_disposition.discharge_disposition_description
    , med.place_of_service_code
    , ref_place_of_service.place_of_service_description
    , med.bill_type_code
    , ref_bill_type.bill_type_description
    , med.drg_code_type
    , med.drg_code
    , med.revenue_center_code
    , med.service_unit_quantity
    , med.hcpcs_code
    , med.hcpcs_modifier_1
    , med.hcpcs_modifier_2
    , med.hcpcs_modifier_3
    , med.hcpcs_modifier_4
    , med.hcpcs_modifier_5
    , med.rendering_npi
    , rendering_prov.provider_name as rendering_name
    , med.rendering_tin
    , med.billing_npi
    , billing_prov.provider_name as billing_name
    , med.billing_tin
    , med.facility_npi
    , facility_prov.provider_name as facility_name
    , med.paid_date
    , med.paid_amount
    , med.allowed_amount
    , med.charge_amount
    , med.coinsurance_amount
    , med.copayment_amount
    , med.deductible_amount
    , med.total_cost_amount
    , med.in_network_flag
    , case when member_month.member_month_sk is not null then True else False end as enrollment_flag
    , member_month.member_month_sk
    , service_category.service_category_1
    , service_category.service_category_2
    , service_category.service_category_3
    , enc.encounter_sk
from normalized_input__medical_claim as med
    left outer join npi as rendering_prov
    on med.rendering_npi = rendering_prov.npi
    left outer join npi as billing_prov
    on med.billing_npi = billing_prov.npi
    left outer join npi as facility_prov
    on med.facility_npi = facility_prov.npi
    left outer join service_category__medical_claim_service_category as service_category
    on med.medical_claim_sk = service_category.medical_claim_sk
    and service_category.priority = 1
    left outer join enrollment__medical_claim_member_month as member_month
    on med.medical_claim_sk = member_month.medical_claim_sk
    left outer join encounters__int_crosswalk__claim_encounter as enc
    on med.medical_claim_sk = enc.medical_claim_sk
    and enc.encounter_type_priority = 1
    left outer join {{ ref('tuva_data_assets', 'admit_source') }} as ref_admit_source
    on med.admit_source_code = ref_admit_source.admit_source_code
    left outer join {{ ref('tuva_data_assets', 'admit_type') }} as ref_admit_type
    on med.admit_type_code = ref_admit_type.admit_type_code
    left outer join {{ ref('tuva_data_assets', 'discharge_disposition') }} as ref_discharge_disposition
    on med.discharge_disposition_code = ref_discharge_disposition.discharge_disposition_code
    left outer join {{ ref('tuva_data_assets', 'place_of_service') }} as ref_place_of_service
    on med.place_of_service_code = ref_place_of_service.place_of_service_code
    left outer join {{ ref('tuva_data_assets', 'bill_type') }} as ref_bill_type
    on med.bill_type_code = ref_bill_type.bill_type_code