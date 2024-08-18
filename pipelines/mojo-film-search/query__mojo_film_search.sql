SELECT
    title,
    EXTRACT(YEAR FROM SAFE_CAST(release AS TIMESTAMP)) AS release_year

FROM `a24-portfolio.a24.films`

ORDER BY release_year DESC