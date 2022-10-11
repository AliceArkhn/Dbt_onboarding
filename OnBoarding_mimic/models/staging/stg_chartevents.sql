SELECT *
FROM
    {{ source('mimic2','chartevents') }}