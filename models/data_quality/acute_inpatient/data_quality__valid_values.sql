{{ config(
    enabled = var('claims_enabled', False)
) }}

SELECT
      mc.claim_id
    , mc.claim_line_number
    , mc.bill_type_code
    , CASE
        WHEN bill_type.bill_type_code IS NOT NULL THEN 1
        ELSE 0
    END AS valid_bill_type_code

    , mc.drg_code
    , CASE
        WHEN coalesce(ms_drg.ms_drg_code, apr_drg.apr_drg_code) IS NOT NULL THEN 1
        ELSE 0
    END AS valid_drg_code

    , mc.admit_type_code
    , CASE
        WHEN admit_type.admit_type_code IS NOT NULL THEN 1
        ELSE 0
    END AS valid_admit_type_code

    , mc.admit_source_code
    , CASE
        WHEN admit_source.admit_source_code IS NOT NULL THEN 1
        ELSE 0
    END AS valid_admit_source_code

    , mc.discharge_disposition_code
    , CASE
        WHEN discharge_disposition.discharge_disposition_code IS NOT NULL THEN 1
        ELSE 0
    END AS valid_discharge_disposition_code

    , mc.revenue_center_code
    , CASE
        WHEN revenue_center.revenue_center_code IS NOT NULL THEN 1
        ELSE 0
    END AS valid_revenue_center_code

    , mc.place_of_service_code
    , CASE
        WHEN place_of_service.place_of_service_code IS NOT NULL THEN 1
        ELSE 0
    END AS valid_place_of_service_code

    , mc.admission_date
    , CASE
        WHEN admission.full_date IS NOT NULL THEN 1
        ELSE 0
    END AS valid_admission_date

    , mc.discharge_date
    , CASE
        WHEN discharge.full_date IS NOT NULL THEN 1
        ELSE 0
    END AS valid_discharge_date

    , mc.claim_start_date
    , CASE
        WHEN claim_start.full_date IS NOT NULL THEN 1
        ELSE 0
    END AS valid_claim_start_date

    , mc.claim_end_date
    , CASE
        WHEN claim_end.full_date IS NOT NULL THEN 1
        ELSE 0
    END AS valid_claim_end_date

    , mc.claim_line_start_date
    , CASE
        WHEN claim_line_start.full_date IS NOT NULL THEN 1
        ELSE 0
    END AS valid_claim_line_start_date

    , mc.claim_line_end_date
    , CASE
        WHEN claim_line_end.full_date IS NOT NULL THEN 1
        ELSE 0
    END AS valid_claim_line_end_date

    , mc.diagnosis_code_1
    , CASE
        WHEN icd1.icd_10_cm IS NOT NULL THEN 1
        ELSE 0
    END AS valid_diagnosis_code_1

    , mc.diagnosis_code_2
    , CASE
        WHEN icd2.icd_10_cm IS NOT NULL THEN 1
        ELSE 0
    END AS valid_diagnosis_code_2

    , mc.diagnosis_code_3
    , CASE
        WHEN icd3.icd_10_cm IS NOT NULL THEN 1
        ELSE 0
    END AS valid_diagnosis_code_3

    , mc.diagnosis_code_4
    , CASE
        WHEN icd4.icd_10_cm IS NOT NULL THEN 1
        ELSE 0
    END AS valid_diagnosis_code_4

    , mc.diagnosis_code_5
    , CASE
        WHEN icd5.icd_10_cm IS NOT NULL THEN 1
        ELSE 0
    END AS valid_diagnosis_code_5

    , mc.diagnosis_code_6
    , CASE
        WHEN icd6.icd_10_cm IS NOT NULL THEN 1
        ELSE 0
    END AS valid_diagnosis_code_6

    , mc.diagnosis_code_7
    , CASE
        WHEN icd7.icd_10_cm IS NOT NULL THEN 1
        ELSE 0
    END AS valid_diagnosis_code_7

    , mc.diagnosis_code_8
    , CASE
        WHEN icd8.icd_10_cm IS NOT NULL THEN 1
        ELSE 0
    END AS valid_diagnosis_code_8

    , mc.diagnosis_code_9
    , CASE
        WHEN icd9.icd_10_cm IS NOT NULL THEN 1
        ELSE 0
    END AS valid_diagnosis_code_9

    , mc.diagnosis_code_10
    , CASE
        WHEN icd10.icd_10_cm IS NOT NULL THEN 1
        ELSE 0
    END AS valid_diagnosis_code_10

    , mc.diagnosis_code_11
    , CASE
        WHEN icd11.icd_10_cm IS NOT NULL THEN 1
        ELSE 0
    END AS valid_diagnosis_code_11

    , mc.diagnosis_code_12
    , CASE
        WHEN icd12.icd_10_cm IS NOT NULL THEN 1
        ELSE 0
    END AS valid_diagnosis_code_12

    , mc.diagnosis_code_13
    , CASE
        WHEN icd13.icd_10_cm IS NOT NULL THEN 1
        ELSE 0
    END AS valid_diagnosis_code_13

    , mc.diagnosis_code_14
    , CASE
        WHEN icd14.icd_10_cm IS NOT NULL THEN 1
        ELSE 0
    END AS valid_diagnosis_code_14

    , mc.diagnosis_code_15
    , CASE
        WHEN icd15.icd_10_cm IS NOT NULL THEN 1
        ELSE 0
    END AS valid_diagnosis_code_15

    , mc.diagnosis_code_16
    , CASE
        WHEN icd16.icd_10_cm IS NOT NULL THEN 1
        ELSE 0
    END AS valid_diagnosis_code_16

    , mc.diagnosis_code_17
    , CASE
        WHEN icd17.icd_10_cm IS NOT NULL THEN 1
        ELSE 0
    END AS valid_diagnosis_code_17

    , mc.diagnosis_code_18
    , CASE
        WHEN icd18.icd_10_cm IS NOT NULL THEN 1
        ELSE 0
    END AS valid_diagnosis_code_18

    , mc.diagnosis_code_19
    , CASE
        WHEN icd19.icd_10_cm IS NOT NULL THEN 1
        ELSE 0
    END AS valid_diagnosis_code_19

    , mc.diagnosis_code_20
    , CASE
        WHEN icd20.icd_10_cm IS NOT NULL THEN 1
        ELSE 0
    END AS valid_diagnosis_code_20

    , mc.diagnosis_code_21
    , CASE
        WHEN icd21.icd_10_cm IS NOT NULL THEN 1
        ELSE 0
    END AS valid_diagnosis_code_21

    , mc.diagnosis_code_22
    , CASE
        WHEN icd22.icd_10_cm IS NOT NULL THEN 1
        ELSE 0
    END AS valid_diagnosis_code_22

    , mc.diagnosis_code_23
    , CASE
        WHEN icd23.icd_10_cm IS NOT NULL THEN 1
        ELSE 0
    END AS valid_diagnosis_code_23

    , mc.diagnosis_code_24
    , CASE
        WHEN icd24.icd_10_cm IS NOT NULL THEN 1
        ELSE 0
    END AS valid_diagnosis_code_24

    , mc.diagnosis_code_25
    , CASE
        WHEN icd25.icd_10_cm IS NOT NULL THEN 1
        ELSE 0
    END AS valid_diagnosis_code_25
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('medical_claim') }} mc

left join {{ ref('terminology__bill_type') }} bill_type
    on mc.bill_type_code = bill_type.bill_type_code

left join {{ ref('terminology__ms_drg') }} ms_drg
    on mc.drg_code_type = 'ms-drg'
    and mc.drg_code = ms_drg.ms_drg_code

left join {{ ref('terminology__apr_drg') }} apr_drg
    on mc.drg_code_type = 'apr-drg'
    and mc.drg_code = apr_drg.apr_drg_code

left join {{ ref('terminology__admit_type') }} admit_type
    on mc.admit_type_code = admit_type.admit_type_code

left join {{ ref('terminology__admit_source') }} admit_source
    on mc.admit_source_code = admit_source.admit_source_code

left join {{ ref('terminology__discharge_disposition') }} discharge_disposition
    on mc.discharge_disposition_code = discharge_disposition.discharge_disposition_code

left join {{ ref('terminology__revenue_center') }} revenue_center
    on mc.revenue_center_code = revenue_center.revenue_center_code

left join {{ ref('terminology__place_of_service') }} place_of_service
    on mc.place_of_service_code = place_of_service.place_of_service_code

left join {{ ref('reference_data__calendar') }} admission
    on mc.admission_date = admission.full_date

left join {{ ref('reference_data__calendar') }} discharge
    on mc.discharge_date = discharge.full_date

left join {{ ref('reference_data__calendar') }} claim_start
    on mc.claim_start_date = claim_start.full_date

left join {{ ref('reference_data__calendar') }} claim_end
    on mc.claim_end_date = claim_end.full_date

left join {{ ref('reference_data__calendar') }} claim_line_start
    on mc.claim_line_start_date = claim_line_start.full_date

left join {{ ref('reference_data__calendar') }} claim_line_end
    on mc.claim_line_end_date = claim_line_end.full_date

left join {{ ref('terminology__icd_10_cm') }} icd1
    on mc.diagnosis_code_1 = icd1.icd_10_cm

left join {{ ref('terminology__icd_10_cm') }} icd2
    on mc.diagnosis_code_2 = icd2.icd_10_cm

left join {{ ref('terminology__icd_10_cm') }} icd3
    on mc.diagnosis_code_3 = icd3.icd_10_cm

left join {{ ref('terminology__icd_10_cm') }} icd4
    on mc.diagnosis_code_4 = icd4.icd_10_cm

left join {{ ref('terminology__icd_10_cm') }} icd5
    on mc.diagnosis_code_5 = icd5.icd_10_cm

left join {{ ref('terminology__icd_10_cm') }} icd6
    on mc.diagnosis_code_6 = icd6.icd_10_cm

left join {{ ref('terminology__icd_10_cm') }} icd7
    on mc.diagnosis_code_7 = icd7.icd_10_cm

left join {{ ref('terminology__icd_10_cm') }} icd8
    on mc.diagnosis_code_8 = icd8.icd_10_cm

left join {{ ref('terminology__icd_10_cm') }} icd9
    on mc.diagnosis_code_9 = icd9.icd_10_cm

left join {{ ref('terminology__icd_10_cm') }} icd10
    on mc.diagnosis_code_10 = icd10.icd_10_cm

left join {{ ref('terminology__icd_10_cm') }} icd11
    on mc.diagnosis_code_11 = icd11.icd_10_cm

left join {{ ref('terminology__icd_10_cm') }} icd12
    on mc.diagnosis_code_12 = icd12.icd_10_cm

left join {{ ref('terminology__icd_10_cm') }} icd13
    on mc.diagnosis_code_13 = icd13.icd_10_cm

left join {{ ref('terminology__icd_10_cm') }} icd14
    on mc.diagnosis_code_14 = icd14.icd_10_cm

left join {{ ref('terminology__icd_10_cm') }} icd15
    on mc.diagnosis_code_15 = icd15.icd_10_cm

left join {{ ref('terminology__icd_10_cm') }} icd16
    on mc.diagnosis_code_16 = icd16.icd_10_cm

left join {{ ref('terminology__icd_10_cm') }} icd17
    on mc.diagnosis_code_17 = icd17.icd_10_cm

left join {{ ref('terminology__icd_10_cm') }} icd18
    on mc.diagnosis_code_18 = icd18.icd_10_cm

left join {{ ref('terminology__icd_10_cm') }} icd19
    on mc.diagnosis_code_19 = icd19.icd_10_cm

left join {{ ref('terminology__icd_10_cm') }} icd20
    on mc.diagnosis_code_20 = icd20.icd_10_cm

left join {{ ref('terminology__icd_10_cm') }} icd21
    on mc.diagnosis_code_21 = icd21.icd_10_cm

left join {{ ref('terminology__icd_10_cm') }} icd22
    on mc.diagnosis_code_22 = icd22.icd_10_cm

left join {{ ref('terminology__icd_10_cm') }} icd23
    on mc.diagnosis_code_23 = icd23.icd_10_cm

left join {{ ref('terminology__icd_10_cm') }} icd24
    on mc.diagnosis_code_24 = icd24.icd_10_cm

left join {{ ref('terminology__icd_10_cm') }} icd25
    on mc.diagnosis_code_25 = icd25.icd_10_cm
