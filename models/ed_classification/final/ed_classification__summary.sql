{{ config(
     enabled = var('ed_classification_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}


select
    class.encounter_id
    , cat.classification_name as ed_classification_description
    , cat.classification_order as ed_classification_order
    , class.person_id
    , class.encounter_end_date
    , {{  concat_custom([date_part('year', 'class.encounter_end_date'),
                      dbt.right(
                      concat_custom(["'00'", date_part('month', 'class.encounter_end_date')])
                      , 2)]) }} as year_month
    , class.primary_diagnosis_code
    , class.primary_diagnosis_description
    , class.paid_amount
    , class.allowed_amount
    , class.charge_amount
    , class.facility_id
    , fac_prov.provider_organization_name as facility_name
    , practice_state as facility_state
    , practice_city as facility_city
    , practice_zip_code as facility_zip_code
    , pat.sex as patient_sex
    , floor({{ datediff('pat.birth_date', 'class.encounter_end_date', 'hour') }} / 8766.0) as patient_age
    , zip_code as patient_zip_code
    , latitude as patient_latitude
    , longitude as patient_longitude
    , race as patient_race
from {{ ref('ed_classification__int_filter_encounter_with_classification') }} class
inner join {{ ref('ed_classification__categories') }} cat
    on class.classification = cat.classification
left join {{ ref('terminology__provider') }} fac_prov 
    on class.facility_id = fac_prov.npi
left join {{ ref('ed_classification__stg_patient') }} pat
    on class.person_id = pat.person_id
