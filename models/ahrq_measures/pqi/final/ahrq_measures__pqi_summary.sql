{{ config(
    enabled = var('pqi_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
) }}

select p.pqi_number
 , m.pqi_name
 , e.year_number
 , e.encounter_id
 , e.data_source
 , e.person_id
 , e.facility_id
 , e.drg_code
 , e.drg_description
 , e.encounter_start_date
 , e.encounter_end_date
 , e.length_of_stay
 , e.paid_amount
 , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('ahrq_measures__pqi_num_long') }} as p
inner join {{ ref('ahrq_measures__stg_pqi_inpatient_encounter') }} as e
    on p.encounter_id = e.encounter_id
    and p.data_source = e.data_source
inner join {{ ref('pqi__measures') }} as m on cast(p.pqi_number as {{ dbt.type_string() }}) = m.pqi_number
