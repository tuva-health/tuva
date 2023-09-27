

-- select *
-- From value_sets.value_set_codes
-- where code in (
--     '99202', '99203', '99204', '99205',
--     '99212', '99213', '99214', '99215', '99341', '99342', '99344', '99345', '99347', '99348', '99349', '99350', '99386', '99387'
--     )
with encounters_visits as (
    select *
    From TUVA_TEST.core.ENCOUNTER
    where ENCOUNTER_TYPE in (
'home health',
'office visit',
'outpatient',
'outpatient rehabilitation',
'telehealth'
)
)

,
procedure_encounters as (
    select * from TUVA_TEST.CORE.PROCEDURE
)

select * From DEVFORREST.