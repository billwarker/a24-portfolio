SELECT
    title,
    EXTRACT(DATE FROM SAFE_CAST(release AS TIMESTAMP)) AS release_date

FROM `a24-portfolio.a24.films`

ORDER BY release_date DESC