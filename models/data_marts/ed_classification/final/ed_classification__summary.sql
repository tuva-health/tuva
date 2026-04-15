{{ config(
     enabled = var('ed_classification_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}


select
    encounter.encounter_id
    , cat.classification_name as ed_classification_description
    , cat.classification_order as ed_classification_order
    , encounter.person_id
    , encounter.encounter_end_date
    , {{ concat_custom([date_part('year', 'encounter.encounter_end_date'),
                      dbt.right(
                      concat_custom(["'00'", date_part('month', 'encounter.encounter_end_date')])
                      , 2)]) }} as year_month
    , encounter.primary_diagnosis_code
    , encounter.primary_diagnosis_description
    , encounter.paid_amount
    , encounter.allowed_amount
    , encounter.charge_amount
    , encounter.facility_npi
    , fac_prov.provider_organization_name as facility_name
    , practice_state as facility_state
    , practice_city as facility_city
    , practice_zip_code as facility_zip_code
    , pat.sex as patient_sex
    , floor({{ datediff('pat.birth_date', 'encounter.encounter_end_date', 'hour') }} / 8766.0) as patient_age
    , zip_code as patient_zip_code
    , latitude as patient_latitude
    , longitude as patient_longitude
    , race as patient_race
    , pat.data_source
from {{ ref('core__encounter') }} as encounter
left outer join {{ ref('ed_classification__int_filter_encounter_with_classification') }} as class
    on encounter.encounter_id = class.encounter_id
left outer join {{ ref('ed_classification__categories') }} as cat
    on class.classification = cat.classification
left outer join {{ ref('provider_data__provider') }} as fac_prov
    on encounter.facility_npi = fac_prov.npi
left outer join {{ ref('core__patient') }} as pat
    on encounter.person_id = pat.person_id
where encounter.encounter_type = 'emergency department'
