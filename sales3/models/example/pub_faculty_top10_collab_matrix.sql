
WITH top10_faculty AS (
    SELECT
        facultyid,
        total_publications,
        total_unique_coauthors
    FROM {{ ref('pub_faculty_collaboration_summary') }}
    WHERE type = 'TOTAL'  -- Solo la fila de totales
      AND facultyid IS NOT NULL
    ORDER BY total_publications DESC
    LIMIT 10
),

-- Colaboraciones solo entre faculty del top 10 (faculty vs faculty)
faculty_to_faculty_collab AS (
    SELECT
        w.facultyid AS facultyid_principal,
        a.coauthor_facultyid AS coauthor_faculty,
        COUNT(*) AS collab_count
    FROM {{ ref('pub_activities_wide') }} w
    INNER JOIN {{ ref('pub_activity_authors') }} a
        ON CAST(w.activityid AS STRING) = CAST(a.activityid AS STRING)
    WHERE 
        a.coauthor_facultyid IS NOT NULL
        AND w.facultyid IN (SELECT facultyid FROM top10_faculty)
        AND a.coauthor_facultyid IN (SELECT facultyid FROM top10_faculty)
        AND w.facultyid != a.coauthor_facultyid  -- Excluir auto-referencias
    GROUP BY w.facultyid, a.coauthor_facultyid
),

-- Ranking de colaboradores dentro del top 10
top_collabs_ranked AS (
    SELECT
        facultyid_principal,
        coauthor_faculty,
        collab_count,
        ROW_NUMBER() OVER (
            PARTITION BY facultyid_principal 
            ORDER BY collab_count DESC, coauthor_faculty
        ) AS rank
    FROM faculty_to_faculty_collab
)

SELECT
    t10.facultyid,
    t10.total_publications,
    t10.total_unique_coauthors,
    
    -- Top 3 colaboradores dentro del top 10
    MAX(CASE WHEN tcr.rank = 1 THEN tcr.coauthor_faculty END) AS top_collab_faculty_1_id,
    MAX(CASE WHEN tcr.rank = 1 THEN tcr.collab_count END) AS top_collab_faculty_1_count,
    
    MAX(CASE WHEN tcr.rank = 2 THEN tcr.coauthor_faculty END) AS top_collab_faculty_2_id,
    MAX(CASE WHEN tcr.rank = 2 THEN tcr.collab_count END) AS top_collab_faculty_2_count,
    
    MAX(CASE WHEN tcr.rank = 3 THEN tcr.coauthor_faculty END) AS top_collab_faculty_3_id,
    MAX(CASE WHEN tcr.rank = 3 THEN tcr.collab_count END) AS top_collab_faculty_3_count,
    
    CURRENT_TIMESTAMP() AS created_at
    
FROM top10_faculty t10
LEFT JOIN top_collabs_ranked tcr
    ON t10.facultyid = tcr.facultyid_principal AND tcr.rank <= 3
GROUP BY 
    t10.facultyid, 
    t10.total_publications, 
    t10.total_unique_coauthors
ORDER BY t10.total_publications DESC