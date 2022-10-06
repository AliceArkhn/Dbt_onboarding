{{ config(materialized='table') }}

WITH patients as (
    select
        "ROW_ID",
        "SUBJECT_ID",
        "GENDER",
        "DOB"
    from {{ ref('stg_patients') }}
),
    chartevents as(
        select
            "SUBJECT_ID",
            "HADM_ID",
            "VALUENUM"
        from {{ref('stg_chartevents')}}
    ),

    admissions as (
        select
            "ROW_ID",
            "SUBJECT_ID",
            "ADMITTIME"
        from {{ref('stg_admissions')}}
    ),

    creatinemia as(
        select
            patients."ROW_ID",
            patients."DOB",
            patients."GENDER",
            admissions."SUBJECT_ID"
        from
            patients
        inner join
            admissions
        on
           admissions."SUBJECT_ID" = patients."SUBJECT_ID"
    ),

    final as (
    select
        "ROW_ID",
        "SUBJECT_ID",
        "GENDER"
    from creatinemia
)
select * from final
