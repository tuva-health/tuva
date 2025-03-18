select distinct
    tccl.person_id
    ,csk.condition_sk
from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }} tccl
inner join {{ ref('power_bi__dim_condition') }} csk on tccl.condition = csk.condition