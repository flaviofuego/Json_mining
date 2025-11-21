SELECT activityid, json_content as raw_json
FROM {{ source('json_dbt', 'tabla_base') }}


