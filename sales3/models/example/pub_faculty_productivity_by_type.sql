

WITH faculty_publications AS (
  SELECT
    facultyid,
    type AS publication_type,
    COUNT(*) AS total_publications
  FROM {{ ref('pub_activities_wide') }}
  WHERE facultyid IS NOT NULL
    AND type IS NOT NULL
  GROUP BY facultyid, type
)

SELECT 
  *,
  -- Calcular el total sumando todas las columnas
  COALESCE(`Artistic Works and Performances`, 0) +
  COALESCE(Book, 0) +
  COALESCE(`Case Study`, 0) +
  COALESCE(Chapter, 0) +
  COALESCE(`Conference Proceeding`, 0) +
  COALESCE(`Creative Publications`, 0) +
  COALESCE(`Instructional Publications`, 0) +
  COALESCE(`Journal Publication`, 0) +
  COALESCE(`Media Contribution`, 0) +
  COALESCE(`Other Works`, 0) +
  COALESCE(Patent, 0) +
  COALESCE(`Poster Presentation`, 0) +
  COALESCE(Presentation, 0) +
  COALESCE(Review, 0) AS total_publications
FROM faculty_publications
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
ORDER BY total_publications DESC