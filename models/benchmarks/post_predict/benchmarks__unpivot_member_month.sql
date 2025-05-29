with unpivoted as (
    select 
        year_month
        , person_id
        , payer
        , {{ quote_column('plan') }} 
        , data_source
        , metric
        , value
    from (
        select 
            year_month
            , person_id
            , payer
            , {{ quote_column('plan') }}
            , data_source
            , cast(actual_paid_amount as float) as actual_paid_amount
            , cast(actual_inpatient_paid_amount as float) as actual_inpatient_paid_amount
            , cast(actual_outpatient_paid_amount as float) as actual_outpatient_paid_amount
            , cast(actual_office_based_paid_amount as float) as actual_office_based_paid_amount
            , cast(actual_other_paid_amount as float) as actual_other_paid_amount
 
            , cast(actual_inpatient_encounter_count as float) as actual_inpatient_encounter_count
            , cast(actual_outpatient_encounter_count as float) as actual_outpatient_encounter_count
            , cast(actual_office_based_encounter_count as float) as actual_office_based_encounter_count
            , cast(actual_other_encounter_count as float) as actual_other_encounter_count
            , cast(actual_acute_inpatient_encounter_count as float) as actual_acute_inpatient_encounter_count
            , cast(actual_ed_encounter_count as float) as actual_ed_encounter_count
            , cast(actual_inpatient_skilled_nursing_encounter_count as float) as actual_inpatient_skilled_nursing_encounter_count
            , cast(actual_office_visit_encounter_count as float) as actual_office_visit_encounter_count
            , cast(actual_home_health_encounter_count as float) as actual_home_health_encounter_count
            , cast(actual_snf_encounter_count as float) as actual_snf_encounter_count
            
            , cast(expected_paid_amount as float) as expected_total_paid_amount 
            , cast(expected_inpatient_paid_amount as float) as expected_inpatient_paid_amount
            , cast(expected_outpatient_paid_amount as float) as expected_outpatient_paid_amount
            , cast(expected_office_based_paid_amount as float) as expected_office_based_paid_amount
            , cast(expected_other_paid_amount as float) as expected_other_paid_amount

            , cast(expected_inpatient_encounter_count as float) as expected_inpatient_encounter_count
            , cast(expected_outpatient_encounter_count as float) as expected_outpatient_encounter_count
            , cast(expected_office_based_encounter_count as float) as expected_office_based_encounter_count
            , cast(expected_other_encounter_count as float) as expected_other_encounter_count
            , cast(expected_acute_inpatient_encounter_count as float) as expected_acute_inpatient_encounter_count
            , cast(expected_ed_encounter_count as float) as expected_ed_encounter_count
            , cast(expected_inpatient_skilled_nursing_encounter_count as float) as expected_inpatient_skilled_nursing_encounter_count
            , cast(expected_office_visit_encounter_count as float) as expected_office_visit_encounter_count
            , cast(expected_home_health_encounter_count as float) as expected_home_health_encounter_count
            , cast(expected_snf_encounter_count as float) as expected_snf_encounter_count
        from {{ ref('benchmarks__predict_member_month') }}
    ) as casted_data
    unpivot (
        value for metric in (
             actual_paid_amount
             , actual_inpatient_paid_amount
             , actual_outpatient_paid_amount
             , actual_office_based_paid_amount
             , actual_other_paid_amount
             , actual_ancillary_paid_amount

             , actual_inpatient_encounter_count
             , actual_outpatient_encounter_count
             , actual_office_based_encounter_count
             , actual_other_encounter_count
             , actual_acute_inpatient_encounter_count
             , actual_ed_encounter_count
             , actual_inpatient_skilled_nursing_encounter_count
             , actual_office_visit_encounter_count
             , actual_home_health_encounter_count
             , actual_snf_encounter_count

             , expected_paid_amount
             , expected_inpatient_paid_amount
             , expected_outpatient_paid_amount
             , expected_office_based_paid_amount
             , expected_other_paid_amount
             , expected_ancillary_paid_amount
             
             , expected_inpatient_encounter_count
             , expected_outpatient_encounter_count
             , expected_office_based_encounter_count
             , expected_other_encounter_count
             , expected_acute_inpatient_encounter_count
             , expected_ed_encounter_count
             , expected_inpatient_skilled_nursing_encounter_count
             , expected_office_visit_encounter_count
             , expected_home_health_encounter_count
             , expected_snf_encounter_count
        )
    ) as unpvt
)
, labeled as (
    select 
        year_month
        , person_id
        , payer
        , {{ quote_column('plan') }}
        , data_source
        , metric
        , value
        -- Determine the metric type from the original metric name
        , case 
            when metric like 'actual%' then 'actual'
            when metric like 'expected%' then 'expected'
            else null
        end as metric_type
    from unpivoted
)
select 
    year_month
    , person_id
    , payer
    , {{ quote_column('plan') }}
    , data_source
    , case 
        when metric = 'actual_paid_amount' then 'total paid amount'
        when metric = 'expected_paid_amount' then 'total paid amount'

        when metric = 'actual_inpatient_paid_amount' then 'inpatient paid amount'
        when metric = 'expected_inpatient_paid_amount' then 'inpatient paid amount'

        when metric = 'actual_outpatient_paid_amount' then 'outpatient paid amount'
        when metric = 'expected_outpatient_paid_amount' then 'outpatient paid amount'

        when metric = 'actual_office_based_paid_amount' then 'office based paid amount'
        when metric = 'expected_office_based_paid_amount' then 'office based paid amount'

        when metric = 'actual_other_paid_amount' then 'other paid amount'
        when metric = 'expected_other_paid_amount' then 'other paid amount'
        
        when metric = 'actual_ancillary_paid_amount' then 'ancillary paid amount'
        when metric = 'expected_ancillary_paid_amount' then 'ancillary paid amount'

        when metric = 'actual_inpatient_encounter_count' then 'inpatient encounters'
        when metric = 'expected_inpatient_encounter_count' then 'inpatient encounters'

        when metric = 'actual_outpatient_encounter_count' then 'outpatient encounters'
        when metric = 'expected_outpatient_encounter_count' then 'outpatient encounters'

        when metric = 'actual_office_based_encounter_count' then 'office based encounters'
        when metric = 'expected_office_based_encounter_count' then 'office based encounters'

        when metric = 'actual_other_encounter_count' then 'other encounters'
        when metric = 'expected_other_encounter_count' then 'other encounters'
        
        when metric = 'actual_acute_inpatient_encounter_count' then 'acute inpatient encounters'
        when metric = 'expected_acute_inpatient_encounter_count' then 'acute inpatient encounters'

        when metric = 'actual_ed_encounter_count' then 'ed encounters'
        when metric = 'expected_ed_encounter_count' then 'ed encounters'

        when metric = 'actual_inpatient_skilled_nursing_encounter_count' then 'inpatient skilled nursing encounters'
        when metric = 'expected_inpatient_skilled_nursing_encounter_count' then 'inpatient skilled nursing encounters'

        when metric = 'actual_office_visit_encounter_count' then 'office visit encounters'
        when metric = 'expected_office_visit_encounter_count' then 'office visit encounters'

        when metric = 'actual_home_health_encounter_count' then 'home health encounters'
        when metric = 'expected_home_health_encounter_count' then 'home health encounters'

        when metric = 'actual_snf_encounter_count' then 'snf encounters'
        when metric = 'expected_snf_encounter_count' then 'snf encounters'
        
        else null 
    end as metric
    , value
    , metric_type
from labeled