{% if var('outcome_measures_enabled', False) == true  -%}

with long_num as (
SELECT
    data_source,
    payer_type,
    year_month,
    sum(member_months) as member_months,
    case 
        when measure_name = 'TOTAL_PAID' then 'PMPM' 
        when measure_name = 'MEDICAL_PAID' then 'PMPM'
        when measure_name = 'INPATIENT_PAID' then 'PMPM'
        when measure_name = 'OUTPATIENT_PAID' then 'PMPM'
        when measure_name = 'OFFICE_VISIT_PAID' then 'PMPM'
        when measure_name = 'ANCILLARY_PAID' then 'PMPM'
        when measure_name = 'OTHER_PAID' then 'PMPM'
        when measure_name = 'PHARMACY_PAID' then 'PMPM'
        when measure_name = 'TOTAL_LOS' then 'Encounter'
        when measure_name = 'READMIT_NUM' then 'Encounter'
        when measure_name = 'INPATIENT_ADMIT_COUNT' then 'PKPY'
        when measure_name = 'ED_VISIT_COUNT' then 'PKPY'
        else measure_name 
    end as measure_type,
    case 
        when measure_name = 'TOTAL_PAID' then 'Total PMPM' 
        when measure_name = 'MEDICAL_PAID' then 'Medical PMPM'
        when measure_name = 'INPATIENT_PAID' then 'Inpatient PMPM'
        when measure_name = 'OUTPATIENT_PAID' then 'Outpatient PMPM'
        when measure_name = 'OFFICE_VISIT_PAID' then 'Office Visit PMPM'
        when measure_name = 'ANCILLARY_PAID' then 'Ancillary PMPM'
        when measure_name = 'OTHER_PAID' then 'Other PMPM'
        when measure_name = 'PHARMACY_PAID' then 'Pharmacy PMPM'
        when measure_name = 'TOTAL_LOS' then 'LOS'
        when measure_name = 'READMIT_NUM' then '30 Day Readmit Rate'
        when measure_name = 'INPATIENT_ADMIT_COUNT' then 'Inpatient PKPY'
        when measure_name = 'ED_VISIT_COUNT' then 'ED PKPY'
        else measure_name 
    end as measure_name,
    sum(measure_num) as measure_num
FROM
    {{ ref('outcome_measures__member_month_fact_wide') }}
    UNPIVOT (
        measure_num FOR measure_name IN (
            total_paid, 
            medical_paid,
            inpatient_paid,
            outpatient_paid,
            office_visit_paid,
            ancillary_paid,
            other_paid,
            pharmacy_paid,
            total_los,
            readmit_num,
            inpatient_admit_count,
            ed_visit_count
        )
    ) AS unpivoted_data
GROUP BY
    data_source,
    payer_type,
    year_month,
    measure_type,
    measure_name
),
long_denom as (
SELECT
    data_source,
    payer_type,
    year_month,
    'Encounter' as measure_type,
    case
        when measure_name = 'INPATIENT_ADMIT_COUNT' then 'LOS'
        when measure_name = 'READMIT_DENOM' then '30 Day Readmit Rate'
        else measure_name
    end as measure_name,
    sum(measure_denom) as measure_denom
FROM
    {{ ref('outcome_measures__member_month_fact_wide') }}
    UNPIVOT (
        measure_denom FOR measure_name IN (
            inpatient_admit_count,
            readmit_denom
        )
    ) AS unpivoted_data
GROUP BY
    data_source,
    payer_type,
    year_month,
    measure_type,
    measure_name
)
select
    ln.data_source,
    ln.payer_type,
    ln.year_month,
    ln.measure_type,
    ln.measure_name,
    ln.measure_num,
    case 
        when ln.measure_type = 'PMPM' then ln.member_months
        when ln.measure_type = 'PKPY' then ln.member_months / 12000
        else ld.measure_denom
    end as measure_denom,
    mrr.lower_bound,
    mrr.upper_bound
from long_num ln
    left join long_denom ld on ln.data_source = ld.data_source
        and ln.year_month = ld.year_month
        and ln.measure_name = ld.measure_name
    left join {{ ref('intelligence__crosswalk_measure_reasonable_ranges') }} mrr on ln.payer_type = mrr.payer_type
        and ln.measure_name = mrr.measure_name

{% else -%}

select
    NULL AS data_source,
    NULL AS payer_type,
    NULL AS year_month,
    NULL AS measure_type,
    NULL AS measure_name,
    NULL AS measure_num,
    NULL AS measure_denom,
    NULL AS lower_bound,
    NULL AS upper_bound
from VALUES(1)

{%- endif %}