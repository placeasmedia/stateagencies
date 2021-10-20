SELECT
  cb.gid,
  bd.kind,
  COUNT(bd.geom) num_pts
FROM
  `vigilant-balm-122322.POIChina.countybound` cb,
  `vigilant-balm-122322.POIChina.baidu2015` bd
WHERE
--   ST_Covers(county.geom, ST_GeogPoint(gaode.gpsx, gaode.gpsy))
  ST_Covers(county.geom, bd.geom)
GROUP BY
  cb.gid, bd.kind
ORDER BY
  num_pts DESC