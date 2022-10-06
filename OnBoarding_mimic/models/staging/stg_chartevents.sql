SELECT *
FROM
    {{ source('mimic','CHARTEVENTS') }}