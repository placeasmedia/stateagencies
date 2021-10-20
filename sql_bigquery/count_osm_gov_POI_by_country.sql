--define bounding area of each country
WITH bounding_area AS (SELECT 
  osm_id, geometry,
  st_area(geometry) as area
FROM `bigquery-public-data.geo_openstreetmap.planet_features`
WHERE 
  feature_type="multipolygons"
  AND ('admin_level', '2') IN (SELECT (key, value) FROM unnest(all_tags))
  AND ('boundary', 'administrative') IN (SELECT (key, value) FROM unnest(all_tags))
ORDER BY area desc
)
SELECT bounding_area.osm_id, poi.poitype, count(poi.id) as num_gov --count number of POIs by type and country
FROM
(
SELECT nodes.id, osm_timestamp,  tags.value as poitype, geometry -- select government POIs
FROM `bigquery-public-data.geo_openstreetmap.planet_nodes` AS nodes
JOIN UNNEST(all_tags) AS tags
WHERE (tags.key = 'amenity'
  AND tags.value IN ("police", 
  "fire_station", 
  "post_office", 
  "townhall", 
  "courthouse", 
  "prison", 
  "embassy", 
  "community_centre", 
  "public_building")
)
) poi, bounding_area 
WHERE ST_DWithin(bounding_area.geometry, poi.geometry, 0)
GROUP by bounding_area.osm_id, poi.poitype