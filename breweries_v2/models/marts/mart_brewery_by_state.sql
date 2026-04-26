SELECT
state_province,
COUNT(DISTINCT id) AS breweries,
SUM(CASE WHEN brewery_type = 'micro' THEN 1 ELSE 0 END) AS micro_breweries,
SUM(CASE WHEN brewery_type = 'closed' THEN 1 ELSE 0 END) AS closed_breweries

FROM {{ ref('stg_breweries_v2') }}

GROUP BY
state_province