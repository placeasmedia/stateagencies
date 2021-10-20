SELECT td.gid, count(gd.gid) numpts
FROM 
  `vigilant-balm-122322.POIChina.gaode2018` gd,
  `vigilant-balm-122322.POIChina.townbound` td
WHERE 
  (gd.typecode like '%130105%' or gd.typecode like '%130106%' or gd.typecode like '%130501%' or gd.typecode like '%130502%' or gd.typecode like '%130503%' or gd.typecode like '%130505%' or gd.typecode like '%130506%' or gd.typecode like '%130701%' or gd.typecode like '%130702%' or gd.typecode like '%130703%')
  AND ST_CONTAINS(td.geom, ST_GEOGPOINT(gd.locationx, gd.locationy))
GROUP BY
  td.gid