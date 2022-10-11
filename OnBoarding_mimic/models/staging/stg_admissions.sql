SELECT *
FROM
    {{ source('mimic','ADMISSIONS') }}

