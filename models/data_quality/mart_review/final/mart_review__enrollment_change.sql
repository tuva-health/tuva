WITH RankedMonths AS (
    SELECT
        Patient_ID,
        YEAR_MONTH,
        data_source,
        LAG(year_month_date, 1) OVER (PARTITION BY Patient_ID, data_source ORDER BY year_month_date) AS Prev_YEAR_MONTH,
        LEAD(year_month_date, 1) OVER (PARTITION BY Patient_ID, data_source ORDER BY year_month_date) AS Next_YEAR_MONTH,
        year_month_date
    FROM {{ ref('mart_review__stg_member_month') }} 
),
Changes AS (
 SELECT
    Patient_ID,
    data_source,
    year_month_date AS Change_MONTH,
    CASE
        WHEN Prev_YEAR_MONTH IS NULL
            OR {{ dateadd('month', -1, 'year_month_date') }} != Prev_YEAR_MONTH
        THEN 'added'
    END AS Change_Type
FROM RankedMonths
UNION ALL
SELECT
    Patient_ID,
    data_source,
    {{ dateadd('month', 1, 'year_month_date') }} AS Change_MONTH,
    CASE
        WHEN Next_YEAR_MONTH IS NULL
            OR {{ dateadd('month', 1, 'year_month_date') }} != Next_YEAR_MONTH
        THEN 'removed'
    END AS Change_Type
FROM RankedMonths

),
Final AS (
    SELECT
        CONCAT(Patient_ID, '|', cast(Change_MONTH as varchar(10))) AS MemberMonthKey,
        data_source,
        Patient_ID,
        Change_MONTH,
        Change_Type
    FROM Changes
    WHERE Change_Type IS NOT NULL
),
Result AS (
    SELECT
        data_source,
        Change_MONTH,
        Change_Type,
        COUNT(*) AS member_count
    FROM Final
    GROUP BY data_source
    , Change_MONTH
    , Change_Type
)


SELECT * , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM Result

