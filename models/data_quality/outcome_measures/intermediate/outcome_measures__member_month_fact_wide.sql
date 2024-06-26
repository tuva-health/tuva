{% if var('outcome_measures_enabled', False) == true  -%}

with pkpy as (
    select
        e.data_source
        ,e.patient_id
        ,date_part(year, e.encounter_end_date) || lpad(date_part(month, encounter_end_date),2,0) as year_month
        ,SUM(case when e.encounter_type = 'acute inpatient' then 1 else null end) as inpatient_admit_count
        ,SUM(case when e.encounter_type = 'emergency department' then 1 else null end) as ed_visit_count
        ,SUM(e.length_of_stay) as total_los
        ,SUM(rs.unplanned_readmit_30_flag) as readmit_num
        ,SUM(rs.index_admission_flag) as readmit_denom
    from {{ source('tuva_core','encounter') }}  e
        left join {{ source('tuva_readmissions','readmission_summary')}} rs on e.patient_id = rs.patient_id // Need to confirm this is the correct way to join, no data_source in RS
            and e.encounter_id = rs.encounter_id
            and rs.index_admission_flag = 1
    group by
        e.data_source
        ,e.patient_id
        ,year_month
),
payers as (
    select distinct
        data_source
        ,payer_type
    from {{ source('tuva_core','eligibility') }}
)
select
    pmp.data_source
    ,payers.payer_type
    ,pmp.year_month
    ,pmp.patient_id
    ,1 AS member_months
    ,CAST(inpatient_paid + outpatient_paid + office_visit_paid + ancillary_paid + other_paid + pharmacy_paid AS REAL) as total_paid
    ,CAST(inpatient_paid + outpatient_paid + office_visit_paid + ancillary_paid + other_paid AS REAL) as medical_paid
    ,CAST(pmp.inpatient_paid AS REAL) as inpatient_paid
    ,CAST(pmp.outpatient_paid AS REAL) as outpatient_paid
    ,CAST(pmp.office_visit_paid AS REAL) as office_visit_paid
    ,CAST(pmp.ancillary_paid AS REAL) as ancillary_paid
    ,CAST(pmp.other_paid AS REAL) as other_paid
    ,CAST(pmp.pharmacy_paid AS REAL) as pharmacy_paid
    ,CAST(COALESCE(pkpy.inpatient_admit_count, 0) AS REAL) AS inpatient_admit_count
    ,CAST(COALESCE(pkpy.ed_visit_count, 0) AS REAL) AS ed_visit_count
    ,CAST(COALESCE(pkpy.total_los, 0) AS REAL) AS total_los // Should I do this or should it be null?
    ,CAST(COALESCE(pkpy.readmit_num, 0) AS REAL) AS readmit_num 
    ,CAST(COALESCE(pkpy.readmit_denom, 0) AS REAL) AS readmit_denom
from {{ source('tuva_financial','pmpm_prep')}} pmp
    left join pkpy on pmp.data_source = pkpy.data_source
        and pmp.year_month = pkpy.year_month
        and pmp.patient_id = pkpy.patient_id
    left join payers on pmp.data_source = payers.data_source

{% else -%}

SELECT 
    NULL AS data_source,
    NULL AS payer_type,
    NULL AS year_month,
    NULL AS patient_id,
    NULL AS member_months,
    NULL AS total_paid,
    NULL AS medical_paid,
    NULL AS inpatient_paid,
    NULL AS outpatient_paid,
    NULL AS office_visit_paid,
    NULL AS ancillary_paid,
    NULL AS other_paid,
    NULL AS pharmacy_paid,
    NULL AS inpatient_admit_count,
    NULL AS ed_visit_count,
    NULL AS total_los,
    NULL AS readmit_num,
    NULL AS readmit_denom
FROM VALUES(1)

{%- endif %}