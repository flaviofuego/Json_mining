

WITH parsed_json AS (
  SELECT
    activityid,
    json_content,
    JSON_EXTRACT_SCALAR(json_content, '$.facultyid') as facultyid,
    JSON_EXTRACT_SCALAR(json_content, '$.userid') as userid,
    JSON_EXTRACT_SCALAR(json_content, '$.fields.Type') as type
   FROM {{ ref('stg_pub_activities_raw')}}

)

SELECT
  activityid,
  facultyid,
  userid,
  type,
  
  -- ===== CAMPOS COMUNES A TODOS LOS TIPOS =====
  -- Campos con "/" o espacios requieren REGEXP_EXTRACT
  REGEXP_EXTRACT(json_content, r'"Actual/Projected Year of Publication/Presentation"\s*:\s*(\d+)') as publication_year,
  REGEXP_EXTRACT(json_content, r'"Description/Abstract"\s*:\s*"([^"]*(?:\\.[^"]*)*)"') as description_abstract,
  JSON_EXTRACT_SCALAR(json_content, '$.fields.Origin') as origin,
  REGEXP_EXTRACT(json_content, r'"Web Address"\s*:\s*"([^"]*)"') as web_address,
  REGEXP_EXTRACT(json_content, r'"Web address"\s*:\s*"([^"]*)"') as web_address_alt,
  
  -- ===== ARTISTIC WORKS AND PERFORMANCES =====
  REGEXP_EXTRACT(json_content, r'"Co-contributor"\s*:\s*"([^"]*)"') as co_contributor,
  REGEXP_EXTRACT(json_content, r'"Date or Date Range"\s*:\s*"([^"]*)"') as date_or_date_range,
  REGEXP_EXTRACT(json_content, r'"Name of Performing Group"\s*:\s*"([^"]*)"') as name_of_performing_group,
  REGEXP_EXTRACT(json_content, r'"Name of Venue/Sponsor"\s*:\s*"([^"]*)"') as name_of_venue_sponsor,
  REGEXP_EXTRACT(json_content, r'"Venue / Sponsor Location \(City, State, Country\)"\s*:\s*"([^"]*)"') as venue_sponsor_location,
  REGEXP_EXTRACT(json_content, r'"Work/Exhibit Title"\s*:\s*"([^"]*)"') as work_exhibit_title,
  
  -- ===== BOOK =====
  REGEXP_EXTRACT(json_content, r'"Author\(s\) / Editor\(s\)"\s*:\s*"([^"]*)"') as authors_editors,
  REGEXP_EXTRACT(json_content, r'"Author/Editor of Larger Work"\s*:\s*"([^"]*)"') as author_editor_larger_work,
  JSON_EXTRACT_SCALAR(json_content, '$.fields.Edition') as edition,
  JSON_EXTRACT_SCALAR(json_content, '$.fields.ISBN') as isbn,
  REGEXP_EXTRACT(json_content, r'"Publisher Location"\s*:\s*"([^"]*)"') as publisher_location,
  JSON_EXTRACT_SCALAR(json_content, '$.fields.Publisher') as publisher,
  REGEXP_EXTRACT(json_content, r'"Title of Larger Work"\s*:\s*"([^"]*)"') as title_of_larger_work,
  JSON_EXTRACT_SCALAR(json_content, '$.fields.Title') as title,
  JSON_EXTRACT_SCALAR(json_content, '$.fields.Volume') as volume,
  
  -- ===== CASE STUDY =====
  REGEXP_EXTRACT(json_content, r'"Author/Editor of larger work"\s*:\s*"([^"]*)"') as author_editor_larger_work_alt,
  JSON_EXTRACT_SCALAR(json_content, '$.fields.Author') as author,
  REGEXP_EXTRACT(json_content, r'"Date Published"\s*:\s*"([^"]*)"') as date_published,
  REGEXP_EXTRACT(json_content, r'"Title of Contribution"\s*:\s*"([^"]*)"') as title_of_contribution,
  REGEXP_EXTRACT(json_content, r'"Title of larger work"\s*:\s*"([^"]*)"') as title_of_larger_work_alt,
  
  -- ===== CHAPTER =====
  REGEXP_EXTRACT(json_content, r'"Book Title"\s*:\s*"([^"]*)"') as book_title,
  REGEXP_EXTRACT(json_content, r'"Chapter Title"\s*:\s*"([^"]*)"') as chapter_title,
  JSON_EXTRACT_SCALAR(json_content, '$.fields.DOI') as doi,
  REGEXP_EXTRACT(json_content, r'"Page Numbers"\s*:\s*"([^"]*)"') as page_numbers,
  
  -- ===== CONFERENCE PROCEEDING =====
  JSON_EXTRACT_SCALAR(json_content, '$.fields.Contributor') as contributor,
  REGEXP_EXTRACT(json_content, r'"ISBN/ISSN #"\s*:\s*"([^"]*)"') as isbn_issn,
  REGEXP_EXTRACT(json_content, r'"Issue Number"\s*:\s*"([^"]*)"') as issue_number,
  REGEXP_EXTRACT(json_content, r'"Month / Season"\s*:\s*"([^"]*)"') as month_season,
  REGEXP_EXTRACT(json_content, r'"Name of Conference"\s*:\s*"([^"]*)"') as name_of_conference,
  
  -- ===== CREATIVE PUBLICATIONS =====
  REGEXP_EXTRACT(json_content, r'"Contributor\(s\)"\s*:\s*"([^"]*)"') as contributors,
  REGEXP_EXTRACT(json_content, r'"Issue Number/Edition"\s*:\s*"([^"]*)"') as issue_number_edition,
  JSON_EXTRACT_SCALAR(json_content, '$.fields.Month') as month,
  REGEXP_EXTRACT(json_content, r'"Page Number or Number of Pages"\s*:\s*"([^"]*)"') as page_number_or_count,
  REGEXP_EXTRACT(json_content, r'"Title of Work"\s*:\s*"([^"]*)"') as title_of_work,
  REGEXP_EXTRACT(json_content, r'"Title of larger work \(if applicable\)"\s*:\s*"([^"]*)"') as title_larger_work_if_applicable,
  
  -- ===== INSTRUCTIONAL PUBLICATIONS =====
  REGEXP_EXTRACT(json_content, r'"Author\(s\)"\s*:\s*"([^"]*)"') as authors,
  REGEXP_EXTRACT(json_content, r'"ISBN/ISSN/ASIN #"\s*:\s*"([^"]*)"') as isbn_issn_asin,
  REGEXP_EXTRACT(json_content, r'"Organization \(if applicable\)"\s*:\s*"([^"]*)"') as organization,
  
  -- ===== JOURNAL PUBLICATION =====
  JSON_EXTRACT_SCALAR(json_content, '$.fields.Journal') as journal,
  REGEXP_EXTRACT(json_content, r'"PubMed Central ID Number"\s*:\s*"([^"]*)"') as pubmed_central_id,
  
  -- ===== MEDIA CONTRIBUTION =====
  REGEXP_EXTRACT(json_content, r'"Activity Type"\s*:\s*"([^"]*)"') as activity_type,
  JSON_EXTRACT_SCALAR(json_content, '$.fields.Date') as date,
  JSON_EXTRACT_SCALAR(json_content, '$.fields.ISSN') as issn,
  JSON_EXTRACT_SCALAR(json_content, '$.fields.Issue') as issue,
  REGEXP_EXTRACT(json_content, r'"Name of Venue"\s*:\s*"([^"]*)"') as name_of_venue,
  REGEXP_EXTRACT(json_content, r'"Page Numbers or Number of Pages"\s*:\s*"([^"]*)"') as page_numbers_or_count,
  REGEXP_EXTRACT(json_content, r'"Title of Contribution \(if applicable\)"\s*:\s*"([^"]*)"') as title_contribution_if_applicable,
  
  -- ===== OTHER WORKS =====
  REGEXP_EXTRACT(json_content, r'"Co-contributor\(s\)"\s*:\s*"([^"]*)"') as co_contributors,
  REGEXP_EXTRACT(json_content, r'"ISBN/ISSN/ASIN Number"\s*:\s*"([^"]*)"') as isbn_issn_asin_number,
  REGEXP_EXTRACT(json_content, r'"Location of Publisher \(City, State, Country\)"\s*:\s*"([^"]*)"') as location_of_publisher,
  REGEXP_EXTRACT(json_content, r'"Outlet \(if applicable\)"\s*:\s*"([^"]*)"') as outlet,
  
  -- ===== PATENT =====
  REGEXP_EXTRACT(json_content, r'"Copyright/Patent Number/ID"\s*:\s*"([^"]*)"') as copyright_patent_number,
  REGEXP_EXTRACT(json_content, r'"If Patent Cooperation Treaty, List Nations"\s*:\s*"([^"]*)"') as pct_nations,
  REGEXP_EXTRACT(json_content, r'"If patent has been assigned, to whom\?"\s*:\s*"([^"]*)"') as patent_assigned_to,
  REGEXP_EXTRACT(json_content, r'"If patent has been licensed, to whom\?"\s*:\s*"([^"]*)"') as patent_licensed_to,
  REGEXP_EXTRACT(json_content, r'"Inventor\(s\)"\s*:\s*"([^"]*)"') as inventors,
  REGEXP_EXTRACT(json_content, r'"Patent Nationality"\s*:\s*"([^"]*)"') as patent_nationality,
  REGEXP_EXTRACT(json_content, r'"Title of Intellectual Property"\s*:\s*"([^"]*)"') as title_intellectual_property,
  JSON_EXTRACT_SCALAR(json_content, '$.fields.URL') as url,
  
  -- ===== POSTER PRESENTATION =====
  REGEXP_EXTRACT(json_content, r'"Conference / Meeting Name"\s*:\s*"([^"]*)"') as conference_meeting_name,
  REGEXP_EXTRACT(json_content, r'"Conference Location"\s*:\s*"([^"]*)"') as conference_location,
  REGEXP_EXTRACT(json_content, r'"Presenter\(s\)"\s*:\s*"([^"]*)"') as presenters,
  REGEXP_EXTRACT(json_content, r'"Sponsoring Organization"\s*:\s*"([^"]*)"') as sponsoring_organization,
  REGEXP_EXTRACT(json_content, r'"Title of Presentation"\s*:\s*"([^"]*)"') as title_of_presentation,
  
  -- ===== PRESENTATION =====
  REGEXP_EXTRACT(json_content, r'"Conference/Meeting Name"\s*:\s*"([^"]*)"') as conference_meeting_name_alt,
  
  -- ===== REVIEW =====
  REGEXP_EXTRACT(json_content, r'"ISSN/ISBN #"\s*:\s*"([^"]*)"') as issn_isbn,
  REGEXP_EXTRACT(json_content, r'"Page Numbers of Number of Pages"\s*:\s*"([^"]*)"') as page_numbers_of_number_pages,
  REGEXP_EXTRACT(json_content, r'"Title of Review"\s*:\s*"([^"]*)"') as title_of_review,
  
  -- Metadata
  CURRENT_TIMESTAMP() as created_at

FROM parsed_json