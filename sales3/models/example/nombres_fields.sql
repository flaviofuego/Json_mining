WITH parsed_json AS (
  SELECT
    activityid,
    JSON_EXTRACT_SCALAR(json_content, '$.fields.Type') as publication_type,
    JSON_EXTRACT(json_content, '$.fields') as fields_obj
  FROM {{ ref('stg_pub_activities_raw')}}  
),
field_keys AS (
  SELECT
    publication_type,
    field_name
  FROM parsed_json,
    UNNEST(REGEXP_EXTRACT_ALL(TO_JSON_STRING(fields_obj), r'"([^"]+)":')) as field_name
  WHERE field_name != 'Type'  -- Excluir el campo Type en sí mismo
),
TYPES AS (
  SELECT DISTINCT
    field_name as fields,
    publication_type as type
  FROM field_keys
  WHERE publication_type IS NOT NULL
)

SELECT
  REPLACE(fields, '\\', '') as fields,
  type
FROM TYPES
ORDER BY type, fields