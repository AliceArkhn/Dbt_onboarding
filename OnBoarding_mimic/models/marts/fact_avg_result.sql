 {{config(materialized='table')}}

WITH patients as (
    select
        "ROW_ID",
        "SUBJECT_ID",
        "GENDER",
        "DOB"
    from
    {{ref('stg_patients')}}
),

admissions as (
    select
        "HADM_ID",
        "SUBJECT_ID",
        "ADMITTIME"
    from
        {{ref('stg_admissions')}}

),

age_patients as (
    select
        patients."SUBJECT_ID",
        admissions."HADM_ID",
        round(extract(day from (admissions."ADMITTIME" - patients."DOB"))/365) as age,
        patients."GENDER"
    from
        patients
    inner join
        admissions
    on admissions."SUBJECT_ID" = patients."SUBJECT_ID"

),

chartevents as(
        select
            "SUBJECT_ID",
            "HADM_ID",
            "VALUENUM"
        from {{ref('stg_chartevents')}}
         where
             "ITEMID" in (1525)
),

patients_creatinemia as (
    select
        age_patients."SUBJECT_ID",
        age_patients."HADM_ID",
        age_patients.age,
        age_patients."GENDER",
        chartevents."VALUENUM"
    from
        age_patients
    inner join
        chartevents
    on chartevents."SUBJECT_ID" = age_patients."SUBJECT_ID"
    and chartevents."HADM_ID" = age_patients."HADM_ID"


),

avg as (
    select
        avg("VALUENUM"),
        "GENDER"
    from
        patients_creatinemia
    group by
            "GENDER"
)

select *
    from avg