{{ config(materialized='table') }}

select
    {{ safe_cast("patient_id",  api.Column.translate_type("string")) }} as patient_id
,   {{ safe_cast("name",  api.Column.translate_type("string")) }} as name
,   {{ safe_cast("gender",  api.Column.translate_type("string")) }} as gender
,   {{ safe_cast("race",  api.Column.translate_type("string")) }} as race
,   {{ safe_cast("ethnicity",  api.Column.translate_type("string")) }} as ethnicity
,   {{ safe_cast("birth_date",  api.Column.translate_type("date")) }} as birth_date
,   {{ safe_cast("death_date",  api.Column.translate_type("date")) }} as death_date
,   {{ safe_cast("death_flag",  api.Column.translate_type("int")) }} as death_flag
,   {{ safe_cast("address",  api.Column.translate_type("string")) }} as address
,   {{ safe_cast("city",  api.Column.translate_type("string")) }} as city
,   {{ safe_cast("state",  api.Column.translate_type("string")) }} as state
,   {{ safe_cast("zip_code",  api.Column.translate_type("int")) }} as zip_code
,   {{ safe_cast("phone",  api.Column.translate_type("string")) }} as phone
,   {{ safe_cast("email",  api.Column.translate_type("string")) }} as email
,   {{ safe_cast("ssn",  api.Column.translate_type("string")) }} as ssn
,   {{ safe_cast("data_source",  api.Column.translate_type("string")) }} as data_source
from {{ source("tuva_core_staging", var('src_patient_table_name')) }}