create or replace view last_hr_heavy_buffer as 
select ogc_fid, id, lat,lon, st_buffer(wkb_geometry, .025)::geometry(polygon, 4326) as wkb_geometry
from last_hr_prcp;

create table roads_flooded as 
select
street_nam,
sum(b.globvalue),
a.geom
from conterlines_poly as a
inner join last_hr_prcp as b 
on st_dwithin(a.geom::geometry(MULTIpolygon, 4326), b.wkb_geometry::geometry(point, 4326), 0.025)
group by a.street_nam, a.geom;

