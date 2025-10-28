{{ config(
     enabled = var('semantic_layer_enabled',False) | as_bool
   )
}}

{% if var('benchmarks_already_created', False) == true -%}
    select
        py.benchmark_key
      , py.year_nbr
      , py.person_id
      , py.payer
      , py.plan
      , py.data_source
      , ep.paid_amount_pred
      , ep.outpatient_paid_amount_pred
      , ep.other_paid_amount_pred
      , ep.office_based_paid_amount_pred
      , ep.inpatient_paid_amount_pred
      , ep.outpatient_injections_paid_amount_pred
      , ep.emergency_department_paid_amount_pred
      , ep.outpatient_radiology_paid_amount_pred
      , ep.outpatient_pt_ot_st_paid_amount_pred
      , ep.outpatient_hospice_paid_amount_pred
      , ep.urgent_care_paid_amount_pred
      , ep.outpatient_hospital_or_clinic_paid_amount_pred
      , ep.home_health_paid_amount_pred
      , ep.dialysis_paid_amount_pred
      , ep.outpatient_rehabilitation_paid_amount_pred
      , ep.outpatient_surgery_paid_amount_pred
      , ep.ambulatory_surgery_center_paid_amount_pred
      , ep.outpatient_psych_paid_amount_pred
      , ep.dme_orphaned_paid_amount_pred
      , ep.orphaned_claim_paid_amount_pred
      , ep.ambulance_orphaned_paid_amount_pred
      , ep.lab_orphaned_paid_amount_pred
      , ep.office_visit_radiology_paid_amount_pred
      , ep.office_visit_paid_amount_pred
      , ep.office_visit_surgery_paid_amount_pred
      , ep.office_visit_other_paid_amount_pred
      , ep.telehealth_paid_amount_pred
      , ep.office_visit_pt_ot_st_paid_amount_pred
      , ep.office_visit_injections_paid_amount_pred
      , ep.acute_inpatient_paid_amount_pred
      , ep.inpatient_hospice_paid_amount_pred
      , ep.inpatient_psych_paid_amount_pred
      , ep.inpatient_rehabilitation_paid_amount_pred
      , ep.inpatient_skilled_nursing_paid_amount_pred
      , coalesce(ep.inpatient_count_pred, 0)
        + coalesce(ep.office_based_count_pred, 0) 
        + coalesce(ep.other_count_pred, 0)
        + coalesce(ep.outpatient_count_pred, 0) as count_pred
      , ep.inpatient_count_pred
      , ep.office_based_count_pred
      , ep.other_count_pred
      , ep.outpatient_count_pred
      , ep.outpatient_injections_count_pred
      , ep.emergency_department_count_pred
      , ep.outpatient_radiology_count_pred
      , ep.outpatient_pt_ot_st_count_pred
      , ep.outpatient_hospice_count_pred
      , ep.urgent_care_count_pred
      , ep.outpatient_hospital_or_clinic_count_pred
      , ep.home_health_count_pred
      , ep.dialysis_count_pred
      , ep.outpatient_rehabilitation_count_pred
      , ep.outpatient_surgery_count_pred
      , ep.ambulatory_surgery_center_count_pred
      , ep.outpatient_psych_count_pred
      , ep.dme_orphaned_count_pred
      , ep.orphaned_claim_count_pred
      , ep.ambulance_orphaned_count_pred
      , ep.lab_orphaned_count_pred
      , ep.office_visit_radiology_count_pred
      , ep.office_visit_count_pred
      , ep.office_visit_surgery_count_pred
      , ep.office_visit_other_count_pred
      , ep.telehealth_count_pred
      , ep.office_visit_pt_ot_st_count_pred
      , ep.office_visit_injections_count_pred
      , ep.acute_inpatient_count_pred
      , ep.inpatient_hospice_count_pred
      , ep.inpatient_psych_count_pred
      , ep.inpatient_rehabilitation_count_pred
      , ep.inpatient_skilled_nursing_count_pred
      , '{{ var('tuva_last_run') }}' as tuva_last_run
    FROM {{ source('expected_values','person_year') }} as py
    INNER JOIN {{ source('expected_values', 'encounter_predictions') }} as ep
      ON py.benchmark_key = ep.benchmark_key

   UNION ALL

{%- endif %}

  select
      cast(null as {{ dbt.type_string() }}) AS benchmark_key
    , cast(null as DECIMAL) AS year_nbr
    , cast(null as {{ dbt.type_string() }}) AS person_id
    , cast(null as {{ dbt.type_string() }}) AS payer
    , cast(null as {{ dbt.type_string() }}) AS plan
    , cast(null as {{ dbt.type_string() }}) AS data_source
    , cast(null as DECIMAL) AS paid_amount_pred
    , cast(null as DECIMAL) AS outpatient_paid_amount_pred
    , cast(null as DECIMAL) AS other_paid_amount_pred
    , cast(null as DECIMAL) AS office_based_paid_amount_pred
    , cast(null as DECIMAL) AS inpatient_paid_amount_pred
    , cast(null as DECIMAL) AS outpatient_injections_paid_amount_pred
    , cast(null as DECIMAL) AS emergency_department_paid_amount_pred
    , cast(null as DECIMAL) AS outpatient_radiology_paid_amount_pred
    , cast(null as DECIMAL) AS outpatient_pt_ot_st_paid_amount_pred
    , cast(null as DECIMAL) AS outpatient_hospice_paid_amount_pred
    , cast(null as DECIMAL) AS urgent_care_paid_amount_pred
    , cast(null as DECIMAL) AS outpatient_hospital_or_clinic_paid_amount_pred
    , cast(null as DECIMAL) AS home_health_paid_amount_pred
    , cast(null as DECIMAL) AS dialysis_paid_amount_pred
    , cast(null as DECIMAL) AS outpatient_rehabilitation_paid_amount_pred
    , cast(null as DECIMAL) AS outpatient_surgery_paid_amount_pred
    , cast(null as DECIMAL) AS ambulatory_surgery_center_paid_amount_pred
    , cast(null as DECIMAL) AS outpatient_psych_paid_amount_pred
    , cast(null as DECIMAL) AS dme_orphaned_paid_amount_pred
    , cast(null as DECIMAL) AS orphaned_claim_paid_amount_pred
    , cast(null as DECIMAL) AS ambulance_orphaned_paid_amount_pred
    , cast(null as DECIMAL) AS lab_orphaned_paid_amount_pred
    , cast(null as DECIMAL) AS office_visit_radiology_paid_amount_pred
    , cast(null as DECIMAL) AS office_visit_paid_amount_pred
    , cast(null as DECIMAL) AS office_visit_surgery_paid_amount_pred
    , cast(null as DECIMAL) AS office_visit_other_paid_amount_pred
    , cast(null as DECIMAL) AS telehealth_paid_amount_pred
    , cast(null as DECIMAL) AS office_visit_pt_ot_st_paid_amount_pred
    , cast(null as DECIMAL) AS office_visit_injections_paid_amount_pred
    , cast(null as DECIMAL) AS acute_inpatient_paid_amount_pred
    , cast(null as DECIMAL) AS inpatient_hospice_paid_amount_pred
    , cast(null as DECIMAL) AS inpatient_psych_paid_amount_pred
    , cast(null as DECIMAL) AS inpatient_rehabilitation_paid_amount_pred
    , cast(null as DECIMAL) AS inpatient_skilled_nursing_paid_amount_pred
    , cast(null as DECIMAL) AS count_pred
    , cast(null as DECIMAL) AS inpatient_count_pred
    , cast(null as DECIMAL) AS office_based_count_pred
    , cast(null as DECIMAL) AS other_count_pred
    , cast(null as DECIMAL) AS outpatient_count_pred
    , cast(null as DECIMAL) AS outpatient_injections_count_pred
    , cast(null as DECIMAL) AS emergency_department_count_pred
    , cast(null as DECIMAL) AS outpatient_radiology_count_pred
    , cast(null as DECIMAL) AS outpatient_pt_ot_st_count_pred
    , cast(null as DECIMAL) AS outpatient_hospice_count_pred
    , cast(null as DECIMAL) AS urgent_care_count_pred
    , cast(null as DECIMAL) AS outpatient_hospital_or_clinic_count_pred
    , cast(null as DECIMAL) AS home_health_count_pred
    , cast(null as DECIMAL) AS dialysis_count_pred
    , cast(null as DECIMAL) AS outpatient_rehabilitation_count_pred
    , cast(null as DECIMAL) AS outpatient_surgery_count_pred
    , cast(null as DECIMAL) AS ambulatory_surgery_center_count_pred
    , cast(null as DECIMAL) AS outpatient_psych_count_pred
    , cast(null as DECIMAL) AS dme_orphaned_count_pred
    , cast(null as DECIMAL) AS orphaned_claim_count_pred
    , cast(null as DECIMAL) AS ambulance_orphaned_count_pred
    , cast(null as DECIMAL) AS lab_orphaned_count_pred
    , cast(null as DECIMAL) AS office_visit_radiology_count_pred
    , cast(null as DECIMAL) AS office_visit_count_pred
    , cast(null as DECIMAL) AS office_visit_surgery_count_pred
    , cast(null as DECIMAL) AS office_visit_other_count_pred
    , cast(null as DECIMAL) AS telehealth_count_pred
    , cast(null as DECIMAL) AS office_visit_pt_ot_st_count_pred
    , cast(null as DECIMAL) AS office_visit_injections_count_pred
    , cast(null as DECIMAL) AS acute_inpatient_count_pred
    , cast(null as DECIMAL) AS inpatient_hospice_count_pred
    , cast(null as DECIMAL) AS inpatient_psych_count_pred
    , cast(null as DECIMAL) AS inpatient_rehabilitation_count_pred
    , cast(null as DECIMAL) AS inpatient_skilled_nursing_count_pred
    , '{{ var('tuva_last_run') }}' as tuva_last_run