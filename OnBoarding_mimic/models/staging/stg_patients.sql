SELECT *
FROM
    {{ source('mimic','PATIENTS') }}