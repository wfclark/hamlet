
create or replace view last_hr_heavy as 
select ogc_fid, id, lat,lon,
st_buffer(wkb_geometry::geography, 2500) as geom
from last_hr_prcp
where globvalue >= .25;

create or replace view select * from roads, last_hr_heavy where st_dwithin(roads.geom, last_hr_heavy.wkb_geometry, 2500);

SELECT
  a.geom,
  b.geom,
  CASE 
     WHEN ST_Within(a.geom, b.geom::geometry) 
     THEN a.geom
     ELSE ST_Multi(ST_Intersection(a.geom,b.geom::geometry)) 
  END AS geom
FROM roads as a 
join last_hr_heavy as b 
on st_intersects(a.geom, b.geom::geometry);
