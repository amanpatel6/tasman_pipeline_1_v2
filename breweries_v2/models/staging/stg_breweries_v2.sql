SELECT
trim(cast(id as varchar)) as id,
trim(cast(name as varchar)) as brewery_name,
lower(trim(cast(brewery_type as varchar))) as brewery_type,
trim(cast(address_1 as varchar)) as address,
trim(cast(city as varchar)) as city,
trim(cast(state_province as varchar)) as state_province,
trim(cast(postal_code as varchar)) as postal_code,
trim(cast(country as varchar)) as country,
longitude,
latitude,
trim(cast(phone as varchar)) as phone,
trim(cast(website_url as varchar)) as website_url

FROM {{ source('silver', 'breweries_v2') }}

WHERE
id IS NOT NULL
and name IS NOT NULL