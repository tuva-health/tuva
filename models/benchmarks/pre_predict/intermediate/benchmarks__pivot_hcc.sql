{{
    config(
        enabled = var('benchmarks_train', False) | as_bool
    )
}}

with cte as (
    select distinct person_id
, hcc_code
, c.year as year_nbr
from {{ ref('benchmarks__stg_cms_hcc__int_disease_factors') }} as i
left outer join {{ ref('benchmarks__stg_reference_data__calendar') }} as c on i.collection_end_date = c.full_date
where hcc_code is not null
)

, member_month as (
    select mm.person_id
    , cast(left(year_month, 4) as {{ dbt.type_int() }}) as year_nbr
    , count(year_month) as member_month_count
    from {{ ref('benchmarks__stg_core__member_months') }} as mm
    group by mm.person_id
    , cast(left(year_month, 4) as {{ dbt.type_int() }})
)

, condition_flags as (
select
 person_id as condition_person_id
, year_nbr as condition_year_nbr
, max(case when hcc_code = '1' then 1 else 0 end) as hcc_1
, max(case when hcc_code = '2' then 1 else 0 end) as hcc_2
, max(case when hcc_code = '6' then 1 else 0 end) as hcc_6
, max(case when hcc_code = '8' then 1 else 0 end) as hcc_8
, max(case when hcc_code = '9' then 1 else 0 end) as hcc_9
, max(case when hcc_code = '10' then 1 else 0 end) as hcc_10
, max(case when hcc_code = '11' then 1 else 0 end) as hcc_11
, max(case when hcc_code = '12' then 1 else 0 end) as hcc_12
, max(case when hcc_code = '17' then 1 else 0 end) as hcc_17
, max(case when hcc_code = '18' then 1 else 0 end) as hcc_18
, max(case when hcc_code = '19' then 1 else 0 end) as hcc_19
, max(case when hcc_code = '21' then 1 else 0 end) as hcc_21
, max(case when hcc_code = '22' then 1 else 0 end) as hcc_22
, max(case when hcc_code = '23' then 1 else 0 end) as hcc_23
, max(case when hcc_code = '27' then 1 else 0 end) as hcc_27
, max(case when hcc_code = '28' then 1 else 0 end) as hcc_28
, max(case when hcc_code = '29' then 1 else 0 end) as hcc_29
, max(case when hcc_code = '33' then 1 else 0 end) as hcc_33
, max(case when hcc_code = '34' then 1 else 0 end) as hcc_34
, max(case when hcc_code = '35' then 1 else 0 end) as hcc_35
, max(case when hcc_code = '39' then 1 else 0 end) as hcc_39
, max(case when hcc_code = '40' then 1 else 0 end) as hcc_40
, max(case when hcc_code = '46' then 1 else 0 end) as hcc_46
, max(case when hcc_code = '47' then 1 else 0 end) as hcc_47
, max(case when hcc_code = '48' then 1 else 0 end) as hcc_48
, max(case when hcc_code = '51' then 1 else 0 end) as hcc_51
, max(case when hcc_code = '52' then 1 else 0 end) as hcc_52
, max(case when hcc_code = '54' then 1 else 0 end) as hcc_54
, max(case when hcc_code = '55' then 1 else 0 end) as hcc_55
, max(case when hcc_code = '56' then 1 else 0 end) as hcc_56
, max(case when hcc_code = '57' then 1 else 0 end) as hcc_57
, max(case when hcc_code = '58' then 1 else 0 end) as hcc_58
, max(case when hcc_code = '59' then 1 else 0 end) as hcc_59
, max(case when hcc_code = '60' then 1 else 0 end) as hcc_60
, max(case when hcc_code = '70' then 1 else 0 end) as hcc_70
, max(case when hcc_code = '71' then 1 else 0 end) as hcc_71
, max(case when hcc_code = '72' then 1 else 0 end) as hcc_72
, max(case when hcc_code = '73' then 1 else 0 end) as hcc_73
, max(case when hcc_code = '74' then 1 else 0 end) as hcc_74
, max(case when hcc_code = '75' then 1 else 0 end) as hcc_75
, max(case when hcc_code = '76' then 1 else 0 end) as hcc_76
, max(case when hcc_code = '77' then 1 else 0 end) as hcc_77
, max(case when hcc_code = '78' then 1 else 0 end) as hcc_78
, max(case when hcc_code = '79' then 1 else 0 end) as hcc_79
, max(case when hcc_code = '80' then 1 else 0 end) as hcc_80
, max(case when hcc_code = '82' then 1 else 0 end) as hcc_82
, max(case when hcc_code = '83' then 1 else 0 end) as hcc_83
, max(case when hcc_code = '84' then 1 else 0 end) as hcc_84
, max(case when hcc_code = '85' then 1 else 0 end) as hcc_85
, max(case when hcc_code = '86' then 1 else 0 end) as hcc_86
, max(case when hcc_code = '87' then 1 else 0 end) as hcc_87
, max(case when hcc_code = '88' then 1 else 0 end) as hcc_88
, max(case when hcc_code = '96' then 1 else 0 end) as hcc_96
, max(case when hcc_code = '99' then 1 else 0 end) as hcc_99
, max(case when hcc_code = '100' then 1 else 0 end) as hcc_100
, max(case when hcc_code = '103' then 1 else 0 end) as hcc_103
, max(case when hcc_code = '104' then 1 else 0 end) as hcc_104
, max(case when hcc_code = '106' then 1 else 0 end) as hcc_106
, max(case when hcc_code = '107' then 1 else 0 end) as hcc_107
, max(case when hcc_code = '108' then 1 else 0 end) as hcc_108
, max(case when hcc_code = '110' then 1 else 0 end) as hcc_110
, max(case when hcc_code = '111' then 1 else 0 end) as hcc_111
, max(case when hcc_code = '112' then 1 else 0 end) as hcc_112
, max(case when hcc_code = '114' then 1 else 0 end) as hcc_114
, max(case when hcc_code = '115' then 1 else 0 end) as hcc_115
, max(case when hcc_code = '122' then 1 else 0 end) as hcc_122
, max(case when hcc_code = '124' then 1 else 0 end) as hcc_124
, max(case when hcc_code = '134' then 1 else 0 end) as hcc_134
, max(case when hcc_code = '135' then 1 else 0 end) as hcc_135
, max(case when hcc_code = '136' then 1 else 0 end) as hcc_136
, max(case when hcc_code = '137' then 1 else 0 end) as hcc_137
, max(case when hcc_code = '138' then 1 else 0 end) as hcc_138
, max(case when hcc_code = '157' then 1 else 0 end) as hcc_157
, max(case when hcc_code = '158' then 1 else 0 end) as hcc_158
, max(case when hcc_code = '159' then 1 else 0 end) as hcc_159
, max(case when hcc_code = '161' then 1 else 0 end) as hcc_161
, max(case when hcc_code = '162' then 1 else 0 end) as hcc_162
, max(case when hcc_code = '166' then 1 else 0 end) as hcc_166
, max(case when hcc_code = '167' then 1 else 0 end) as hcc_167
, max(case when hcc_code = '169' then 1 else 0 end) as hcc_169
, max(case when hcc_code = '170' then 1 else 0 end) as hcc_170
, max(case when hcc_code = '173' then 1 else 0 end) as hcc_173
, max(case when hcc_code = '176' then 1 else 0 end) as hcc_176
, max(case when hcc_code = '186' then 1 else 0 end) as hcc_186
, max(case when hcc_code = '188' then 1 else 0 end) as hcc_188
, max(case when hcc_code = '189' then 1 else 0 end) as hcc_189
from cte
group by
person_id
, year_nbr

)

select
mm.person_id
, mm.year_nbr
, coalesce(hcc_1, 0) as hcc_1
, coalesce(hcc_2, 0) as hcc_2
, coalesce(hcc_6, 0) as hcc_6
, coalesce(hcc_8, 0) as hcc_8
, coalesce(hcc_9, 0) as hcc_9
, coalesce(hcc_10, 0) as hcc_10
, coalesce(hcc_11, 0) as hcc_11
, coalesce(hcc_12, 0) as hcc_12
, coalesce(hcc_17, 0) as hcc_17
, coalesce(hcc_18, 0) as hcc_18
, coalesce(hcc_19, 0) as hcc_19
, coalesce(hcc_21, 0) as hcc_21
, coalesce(hcc_22, 0) as hcc_22
, coalesce(hcc_23, 0) as hcc_23
, coalesce(hcc_27, 0) as hcc_27
, coalesce(hcc_28, 0) as hcc_28
, coalesce(hcc_29, 0) as hcc_29
, coalesce(hcc_33, 0) as hcc_33
, coalesce(hcc_34, 0) as hcc_34
, coalesce(hcc_35, 0) as hcc_35
, coalesce(hcc_39, 0) as hcc_39
, coalesce(hcc_40, 0) as hcc_40
, coalesce(hcc_46, 0) as hcc_46
, coalesce(hcc_47, 0) as hcc_47
, coalesce(hcc_48, 0) as hcc_48
, coalesce(hcc_51, 0) as hcc_51
, coalesce(hcc_52, 0) as hcc_52
, coalesce(hcc_54, 0) as hcc_54
, coalesce(hcc_55, 0) as hcc_55
, coalesce(hcc_56, 0) as hcc_56
, coalesce(hcc_57, 0) as hcc_57
, coalesce(hcc_58, 0) as hcc_58
, coalesce(hcc_59, 0) as hcc_59
, coalesce(hcc_60, 0) as hcc_60
, coalesce(hcc_70, 0) as hcc_70
, coalesce(hcc_71, 0) as hcc_71
, coalesce(hcc_72, 0) as hcc_72
, coalesce(hcc_73, 0) as hcc_73
, coalesce(hcc_74, 0) as hcc_74
, coalesce(hcc_75, 0) as hcc_75
, coalesce(hcc_76, 0) as hcc_76
, coalesce(hcc_77, 0) as hcc_77
, coalesce(hcc_78, 0) as hcc_78
, coalesce(hcc_79, 0) as hcc_79
, coalesce(hcc_80, 0) as hcc_80
, coalesce(hcc_82, 0) as hcc_82
, coalesce(hcc_83, 0) as hcc_83
, coalesce(hcc_84, 0) as hcc_84
, coalesce(hcc_85, 0) as hcc_85
, coalesce(hcc_86, 0) as hcc_86
, coalesce(hcc_87, 0) as hcc_87
, coalesce(hcc_88, 0) as hcc_88
, coalesce(hcc_96, 0) as hcc_96
, coalesce(hcc_99, 0) as hcc_99
, coalesce(hcc_100, 0) as hcc_100
, coalesce(hcc_103, 0) as hcc_103
, coalesce(hcc_104, 0) as hcc_104
, coalesce(hcc_106, 0) as hcc_106
, coalesce(hcc_107, 0) as hcc_107
, coalesce(hcc_108, 0) as hcc_108
, coalesce(hcc_110, 0) as hcc_110
, coalesce(hcc_111, 0) as hcc_111
, coalesce(hcc_112, 0) as hcc_112
, coalesce(hcc_114, 0) as hcc_114
, coalesce(hcc_115, 0) as hcc_115
, coalesce(hcc_122, 0) as hcc_122
, coalesce(hcc_124, 0) as hcc_124
, coalesce(hcc_134, 0) as hcc_134
, coalesce(hcc_135, 0) as hcc_135
, coalesce(hcc_136, 0) as hcc_136
, coalesce(hcc_137, 0) as hcc_137
, coalesce(hcc_138, 0) as hcc_138
, coalesce(hcc_157, 0) as hcc_157
, coalesce(hcc_158, 0) as hcc_158
, coalesce(hcc_159, 0) as hcc_159
, coalesce(hcc_161, 0) as hcc_161
, coalesce(hcc_162, 0) as hcc_162
, coalesce(hcc_166, 0) as hcc_166
, coalesce(hcc_167, 0) as hcc_167
, coalesce(hcc_169, 0) as hcc_169
, coalesce(hcc_170, 0) as hcc_170
, coalesce(hcc_173, 0) as hcc_173
, coalesce(hcc_176, 0) as hcc_176
, coalesce(hcc_186, 0) as hcc_186
, coalesce(hcc_188, 0) as hcc_188
, coalesce(hcc_189, 0) as hcc_189
from member_month as mm
left outer join condition_flags as f on mm.person_id = f.condition_person_id
and
mm.year_nbr = f.condition_year_nbr
