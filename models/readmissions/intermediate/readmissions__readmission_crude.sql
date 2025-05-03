{{ config(
     enabled = var('readmissions_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

-- Here we calculate readmissions using all encounters
-- that have valid admit and discharge dates and no overlap.
-- This is meant to give a crude sense of the readmission
-- rate without taking into account all the CMS HWR logic.


with encounter_info as (
select
    enc.encounter_id
    , enc.person_id
    , enc.admit_date
    , enc.discharge_date
from {{ ref('readmissions__encounter') }} as enc
left outer join {{ ref('readmissions__encounter_overlap') }} as over_a
    on enc.encounter_id = over_a.encounter_id_a
left outer join {{ ref('readmissions__encounter_overlap') }} as over_b
    on enc.encounter_id = over_b.encounter_id_b
where
    admit_date is not null
    and
    discharge_date is not null
    and
    admit_date <= discharge_date
and over_a.encounter_id_a is null and over_b.encounter_id_b is null
    )


, encounter_sequence as (
select
    encounter_id
    , person_id
    , admit_date
    , discharge_date
    , row_number() over (
        partition by person_id
order by admit_date, discharge_date
    ) as encounter_seq
from encounter_info
)


, readmission_calc as (
select
    aa.encounter_id
    , aa.person_id
    , aa.admit_date
    , aa.discharge_date
    , case
        when bb.encounter_id is not null then 1
	else 0
    end as had_readmission_flag
    , {{ dbt.datediff("bb.admit_date", "aa.discharge_date", "day") }} as days_to_readmit
    , case
        when ({{ dbt.datediff("bb.admit_date", "aa.discharge_date", "day") }}) <= 30 then 1
	else 0
    end as readmit_30_flag
from encounter_sequence as aa left outer join encounter_sequence as bb
     on aa.person_id = bb.person_id
     and aa.encounter_seq + 1 = bb.encounter_seq
)



select *, '{{ var('tuva_last_run') }}' as tuva_last_run
from readmission_calc
