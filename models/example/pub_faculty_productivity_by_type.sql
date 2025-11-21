WITH main_authors AS (
  SELECT DISTINCT
    CAST(activityid AS STRING) AS activityid,
    coauthor_facultyid AS facultyid
  FROM {{ ref('pub_activity_authors')}}  
),

activity_types AS (
  SELECT
    CAST(activityid AS STRING) AS activityid,
    type AS publication_type
  FROM {{ ref('pub_activities_wide')}}  
),

joined AS (
  SELECT
    m.facultyid,
    a.publication_type
  FROM main_authors m
  LEFT JOIN activity_types a USING (activityid)
),

counts AS (
  SELECT
    facultyid,
    publication_type,
    COUNT(*) AS total_publications
  FROM joined
  GROUP BY facultyid, publication_type
)

SELECT *
FROM counts
PIVOT (
  SUM(total_publications)
  FOR publication_type IN (
    'Artistic Works and Performances',
    'Book',
    'Case Study',
    'Chapter',
    'Conference Proceeding',
    'Creative Publications',
    'Instructional Publications',
    'Journal Publication',
    'Media Contribution',
    'Other Works',
    'Patent',
    'Poster Presentation',
    'Presentation',
    'Review'
  )
)
ORDER BY facultyid