/*
Denormalized view of each condition row with additional provider and patient level
information merged on based on the header level detail on the claim
*/

{{ config(
     enabled = var('ed_classification_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

select
    class.encounter_id
    , cat.classification_name
    , cat.classification_order
    , class.patient_id
    , class.encounter_end_date
    , class.primary_diagnosis_code
    , class.primary_diagnosis_description
    , null as primary_ccsr
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
    , pat.sex
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
