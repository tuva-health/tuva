{{ config(
     enabled = var('ed_classification_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}


select
    class.encounter_id
    , cat.classification_name as ed_classification_description
    , cat.classification_order as ed_classification_order
    , class.patient_id
    , class.encounter_end_date
    , cast({{ date_part("year", "class.encounter_end_date") }} as {{ dbt.type_string() }}) 
        || right('0'||cast({{ date_part("month", "class.encounter_end_date") }} as {{ dbt.type_string() }}),2) 
    as year_month
    , class.primary_diagnosis_code
    , class.primary_diagnosis_description
    , ccsr.ccsr_category as primary_ccsr_code
    , ccsr.ccsr_category_description as primary_ccsr_description
    , class.paid_amount
    , class.allowed_amount
    , class.charge_amount
    , class.facility_npi
    , fac_prov.provider_organization_name as facility_name
    , practice_state as facility_state
    , practice_city as facility_city
    , practice_zip_code as facility_zip_code
    , null as facility_latitude
    , null as facility_longitude
    , pat.sex as patient_sex
    , floor({{ datediff('pat.birth_date', 'current_date', 'hour') }} / 8766.0) as patient_age
    , zip_code as patient_zip_code
    , latitude as patient_latitude
    , longitude as patient_longitude
    , race as patient_race
from {{ ref('ed_classification__int_filter_encounter_with_classification') }} class
inner join {{ ref('ed_classification__categories') }} cat
    using(classification)
left join {{ ref('terminology__provider') }} fac_prov 
    on class.facility_npi = fac_prov.npi
left join {{ ref('ed_classification__stg_patient') }} pat
    on class.patient_id = pat.patient_id
left join {{ ref('ccsr__long_condition_category') }} ccsr
    on class.encounter_id = ccsr.encounter_id
    and class.primary_diagnosis_code = ccsr.normalized_code
