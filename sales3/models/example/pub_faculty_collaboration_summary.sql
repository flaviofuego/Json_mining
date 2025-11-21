{{ config(
    materialized='table'
) }}

WITH activities AS (
    SELECT 
        activityid,
        type, 
        facultyid
    FROM {{ ref('pub_activities_wide') }}
    WHERE facultyid IS NOT NULL
      AND type IS NOT NULL
),

authors AS (
    SELECT 
        CAST(activityid AS STRING) AS activityid,
        coauthor_facultyid
    FROM {{ ref('pub_activity_authors') }}
    WHERE coauthor_facultyid IS NOT NULL
),

joined_data AS (
    SELECT
        CAST(activities.activityid AS STRING) AS activityid,
        activities.type,
        activities.facultyid,
        authors.coauthor_facultyid
    FROM activities
    LEFT JOIN authors 
        ON CAST(activities.activityid AS STRING) = authors.activityid
        AND activities.facultyid != authors.coauthor_facultyid  -- ✅ Excluir auto-colaboración
),

-- Resumen general (todas las publicaciones del faculty)
resumen_general AS (
    SELECT
        facultyid,
        'TOTAL' AS type, 
        COUNT(DISTINCT activityid) AS total_publications,
        COUNT(DISTINCT coauthor_facultyid) AS total_unique_coauthors,
        ROUND(SAFE_DIVIDE(
            COUNT(coauthor_facultyid), 
            COUNT(DISTINCT activityid)
        ), 2) AS avg_coauthors_per_publication
    FROM joined_data
    GROUP BY facultyid
),

-- Resumen por cada tipo de publicación
resumen_por_tipo AS (
    SELECT
        facultyid,
        type,
        COUNT(DISTINCT activityid) AS total_publications,
        COUNT(DISTINCT coauthor_facultyid) AS total_unique_coauthors,
        ROUND(SAFE_DIVIDE(
            COUNT(coauthor_facultyid), 
            COUNT(DISTINCT activityid)
        ), 2) AS avg_coauthors_per_publication
    FROM joined_data
    GROUP BY facultyid, type
)

SELECT 
    *,
    CURRENT_TIMESTAMP() AS created_at
FROM resumen_general

UNION ALL

SELECT 
    *,
    CURRENT_TIMESTAMP() AS created_at
FROM resumen_por_tipo

ORDER BY facultyid, type DESC