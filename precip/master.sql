create or replace view last_hr_heavy_buffer as 
select ogc_fid, id, lat,lon, st_buffer(wkb_geometry, .025)::geometry(polygon, 4326) as wkb_geometry
from last_hr_prcp;

create table roads_flooded_bunco as 
select
gid,
street_nam,
sum(b.globvalue),
a.geom
from conterlines_poly as a
inner join last_hr_prcp as b 
on st_dwithin(a.geom::geometry(MULTIpolygon, 4326), b.wkb_geometry::geometry(point, 4326), 0.025)
group by a.street_nam, a.geom;

create table roads_flooded_se_heavy as 
select
gid
street_nam,
sum(b.globvalue),
a.geom
from se_road_polys as a
inner join last_hr_prcp as b 
on st_dwithin(a.geom::geometry(MULTIpolygon, 4326), b.wkb_geometry::geometry(point, 4326), 0.025)
where b.globvalue >= 1
group by a.gid, a.geom;

create table roads_flooded_se_moderate as 
select
gid
street_nam,
sum(b.globvalue),
a.geom
from se_road_polys as a
inner join last_hr_prcp as b 
on st_dwithin(a.geom::geometry(MULTIpolygon, 4326), b.wkb_geometry::geometry(point, 4326), 0.025)
where b.globvalue >= .5
group by a.gid, a.geom;

create table roads_flooded_se_light as 
select
gid
street_nam,
sum(b.globvalue),
a.geom
from se_road_polys as a
inner join last_hr_prcp as b 
on st_dwithin(a.geom::geometry(MULTIpolygon, 4326), b.wkb_geometry::geometry(point, 4326), 0.025)
where b.globvalue >= .25
group by a.gid, a.geom;

create table roads_flooded_se_drizzle as 
select
gid
street_nam,
sum(b.globvalue),
a.geom
from se_road_polys as a
inner join last_hr_prcp as b 
on st_dwithin(a.geom::geometry(MULTIpolygon, 4326), b.wkb_geometry::geometry(point, 4326), 0.025)
where b.globvalue >= .1 and b.globvalue <= .25
group by a.gid, a.geom;


