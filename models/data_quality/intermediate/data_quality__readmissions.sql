{{ config(
     enabled = (var('enable_legacy_data_quality', False) and var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
)}}


with disqualified_unpivot as (
    {{ dbt_utils.unpivot(
        relation=ref('readmissions__encounter_augmented'),
        cast_to=type_string(),  
        exclude=['encounter_id'], 
        field_name='disqualified_reason',
        value_name='flagvalue',
        remove=[
    'encounter_id',
    'person_id',
    'admit_date',
    'discharge_date',
    'discharge_disposition_code',
    'facility_id',
    'drg_code_type',
    'drg_code',
    'paid_amount',
    'length_of_stay',
    'index_admission_flag',
    'planned_flag',
    'specialty_cohort',
    'died_flag',
    'diagnosis_ccs',
    'disqualified_encounter_flag',
    'tuva_last_run'
]
    ) }}
)

-- Using the transformed data to perform aggregation
select 
    {{ concat_custom(["'inpatient encounter '", "d.disqualified_reason"]) }} as data_quality_check
    ,  count(distinct encounter_id) as result_count
      , '{{ var('tuva_last_run') }}' as tuva_last_run
from disqualified_unpivot d
where cast(flagvalue as {{ dbt.type_int() }} ) = 1  
group by disqualified_reason
