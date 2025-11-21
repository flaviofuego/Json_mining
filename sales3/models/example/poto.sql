{{ config(
    materialized='table'
) }}

WITH activity_coauthor_counts AS (
  SELECT
    w.activityid,
    w.facultyid,
    w.type,
    COUNT(a.coauthor_id) as coauthor_count
  FROM {{ ref('pub_activities_wide') }} w
  LEFT JOIN {{ ref('pub_activity_authors') }} a
    ON CAST(w.activityid AS STRING) = CAST(a.activityid AS STRING)
  WHERE w.facultyid IS NOT NULL
  GROUP BY w.activityid, w.facultyid, w.type
),

faculty_basic_stats AS (
  SELECT
    facultyid,
    COUNT(DISTINCT activityid) as total_publications,
    SUM(coauthor_count) as total_coauthor_entries,
    ROUND(SAFE_DIVIDE(SUM(coauthor_count), COUNT(DISTINCT activityid)), 2) as avg_coauthors_per_publication
  FROM activity_coauthor_counts
  GROUP BY facultyid
),

unique_coauthors AS (
  SELECT
    w.facultyid,
    COUNT(DISTINCT a.coauthor_id) as total_unique_coauthors
  FROM {{ ref('pub_activities_wide') }} w
  INNER JOIN {{ ref('pub_activity_authors') }} a
    ON CAST(w.activityid AS STRING) = CAST(a.activityid AS STRING)
  GROUP BY w.facultyid
),

-- Métricas desagregadas por tipo
by_type_stats AS (
  SELECT
    facultyid,
    type,
    COUNT(DISTINCT activityid) as publications_by_type,
    ROUND(AVG(coauthor_count), 2) as avg_coauthors_by_type,
    SUM(coauthor_count) as total_coauthors_by_type
  FROM activity_coauthor_counts
  WHERE coauthor_count > 0
  GROUP BY facultyid, type
)

SELECT
  fs.facultyid,
  fs.total_publications,
  COALESCE(uc.total_unique_coauthors, 0) as total_unique_coauthors,
  fs.avg_coauthors_per_publication,
  
  -- Journal Publication
  MAX(CASE WHEN bt.type = 'Journal Publication' THEN bt.publications_by_type END) as journal_pubs_with_coauthors,
  MAX(CASE WHEN bt.type = 'Journal Publication' THEN bt.avg_coauthors_by_type END) as journal_avg_coauthors,
  MAX(CASE WHEN bt.type = 'Journal Publication' THEN bt.total_coauthors_by_type END) as journal_total_coauthors,
  
  -- Conference Proceeding
  MAX(CASE WHEN bt.type = 'Conference Proceeding' THEN bt.publications_by_type END) as conference_pubs_with_coauthors,
  MAX(CASE WHEN bt.type = 'Conference Proceeding' THEN bt.avg_coauthors_by_type END) as conference_avg_coauthors,
  MAX(CASE WHEN bt.type = 'Conference Proceeding' THEN bt.total_coauthors_by_type END) as conference_total_coauthors,
  
  -- Chapter
  MAX(CASE WHEN bt.type = 'Chapter' THEN bt.publications_by_type END) as chapter_pubs_with_coauthors,
  MAX(CASE WHEN bt.type = 'Chapter' THEN bt.avg_coauthors_by_type END) as chapter_avg_coauthors,
  MAX(CASE WHEN bt.type = 'Chapter' THEN bt.total_coauthors_by_type END) as chapter_total_coauthors,
  
  -- Book
  MAX(CASE WHEN bt.type = 'Book' THEN bt.publications_by_type END) as book_pubs_with_coauthors,
  MAX(CASE WHEN bt.type = 'Book' THEN bt.avg_coauthors_by_type END) as book_avg_coauthors,
  MAX(CASE WHEN bt.type = 'Book' THEN bt.total_coauthors_by_type END) as book_total_coauthors,
  
  -- Presentation
  MAX(CASE WHEN bt.type = 'Presentation' THEN bt.publications_by_type END) as presentation_pubs_with_coauthors,
  MAX(CASE WHEN bt.type = 'Presentation' THEN bt.avg_coauthors_by_type END) as presentation_avg_coauthors,
  MAX(CASE WHEN bt.type = 'Presentation' THEN bt.total_coauthors_by_type END) as presentation_total_coauthors,
  
  CURRENT_TIMESTAMP() as created_at
  
FROM faculty_basic_stats fs
LEFT JOIN unique_coauthors uc ON fs.facultyid = uc.facultyid
LEFT JOIN by_type_stats bt ON fs.facultyid = bt.facultyid
GROUP BY 
  fs.facultyid, 
  fs.total_publications, 
  uc.total_unique_coauthors, 
  fs.avg_coauthors_per_publication
ORDER BY fs.total_publications DESC