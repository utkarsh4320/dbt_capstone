SELECT *
FROM {{ source('raw', 'IPL_MATCH_DATA') }}
