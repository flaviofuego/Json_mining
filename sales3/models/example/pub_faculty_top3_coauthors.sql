

WITH faculty_coauthor_pairs AS (
    SELECT
        w.facultyid AS facultyid_principal,
        a.coauthor_facultyid AS coauthor_id,
        -- Información adicional del coautor
        MAX(CONCAT(COALESCE(a.first_name, ''), ' ', COALESCE(a.last_name, ''))) AS coauthor_name,
        MAX(a.same_school) AS same_school,
        COUNT(*) AS collab_count
    FROM {{ ref('pub_activities_wide') }} w
    INNER JOIN {{ ref('pub_activity_authors') }} a
        ON CAST(w.activityid AS STRING) = CAST(a.activityid AS STRING)
    WHERE a.coauthor_facultyid IS NOT NULL  --  Solo faculty
      AND w.facultyid != a.coauthor_facultyid  --  Excluir auto-colaboración
    GROUP BY w.facultyid, a.coauthor_facultyid
),

ranked_coauthors AS (
    SELECT
        facultyid_principal,
        coauthor_id,
        coauthor_name,
        same_school,
        collab_count,
        ROW_NUMBER() OVER (
            PARTITION BY facultyid_principal 
            ORDER BY collab_count DESC, coauthor_id
        ) AS rank
    FROM faculty_coauthor_pairs
)

SELECT
    facultyid_principal AS facultyid,
    
    -- Top 1
    MAX(CASE WHEN rank = 1 THEN coauthor_id END) AS top1_coauthor_id,
    MAX(CASE WHEN rank = 1 THEN coauthor_name END) AS top1_coauthor_name,
    MAX(CASE WHEN rank = 1 THEN same_school END) AS top1_same_school,
    MAX(CASE WHEN rank = 1 THEN collab_count END) AS top1_collab_count,
    
    -- Top 2
    MAX(CASE WHEN rank = 2 THEN coauthor_id END) AS top2_coauthor_id,
    MAX(CASE WHEN rank = 2 THEN coauthor_name END) AS top2_coauthor_name,
    MAX(CASE WHEN rank = 2 THEN same_school END) AS top2_same_school,
    MAX(CASE WHEN rank = 2 THEN collab_count END) AS top2_collab_count,
    
    -- Top 3
    MAX(CASE WHEN rank = 3 THEN coauthor_id END) AS top3_coauthor_id,
    MAX(CASE WHEN rank = 3 THEN coauthor_name END) AS top3_coauthor_name,
    MAX(CASE WHEN rank = 3 THEN same_school END) AS top3_same_school,
    MAX(CASE WHEN rank = 3 THEN collab_count END) AS top3_collab_count,
    
    CURRENT_TIMESTAMP() AS created_at
    
FROM ranked_coauthors
WHERE rank <= 3
GROUP BY facultyid_principal
ORDER BY top1_collab_count DESC