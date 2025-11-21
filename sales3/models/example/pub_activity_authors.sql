WITH src AS (
  SELECT
    activityid,
    json_content,
    JSON_EXTRACT(json_content, '$.coauthors') AS coauthors_obj
  FROM {{ ref('stg_pub_activities_raw')}}  
),

-- 1. Extraemos las claves del objeto coauthors (164181, 164182, etc.)
keys AS (
  SELECT
    activityid,
    coauthors_obj,
    REGEXP_EXTRACT_ALL(CAST(coauthors_obj AS STRING), r'"(\d+)"') AS key_list
  FROM src
  WHERE coauthors_obj IS NOT NULL
),

-- 2. Convertimos el objeto completo en un array JSON válido eliminando las claves
array_values AS (
  SELECT
    activityid,
    key_list,
    -- Elimina las claves "12345": dejando solo { ... }, { ... }
    CONCAT(
      '[',
      REGEXP_REPLACE(
        REGEXP_REPLACE(CAST(coauthors_obj AS STRING), r'\s*"(\d+)"\s*:', ''),  -- elimina "key":
        r'^\s*\{|\}\s*$', ''                                                    -- elimina llaves exteriores
      ),
      ']'
    ) AS coauthors_array_json
  FROM keys
),

-- 3. Parseamos el array y lo unnestamos
exploded AS (
  SELECT
    activityid,
    key_list[OFFSET(OFFSET)] AS coauthor_index,  -- clave original
    element
  FROM array_values,
  UNNEST(JSON_EXTRACT_ARRAY(coauthors_array_json)) AS element WITH OFFSET
)

SELECT
  activityid,
  SAFE_CAST(coauthor_index AS INT64) AS coauthor_index,

  SAFE_CAST(JSON_VALUE(element, '$.authorid') AS INT64)               AS authorid,
  JSON_VALUE(element, '$.firstname')                                 AS firstname,
  JSON_VALUE(element, '$.middleinitial')                             AS middleinitial,
  JSON_VALUE(element, '$.lastname')                                  AS lastname,
  JSON_VALUE(element, '$.percentcontribution')                       AS percentcontribution,
  SAFE_CAST(JSON_VALUE(element, '$.sameschoolflag') AS INT64)        AS sameschoolflag,
  JSON_VALUE(element, '$.facultyid')                                 AS coauthor_facultyid,
  SAFE_CAST(JSON_VALUE(element, '$.scholarlyactivityid') AS INT64)   AS scholarlyactivityid,

  element AS coauthor_raw_json

FROM exploded
ORDER BY activityid, coauthor_index