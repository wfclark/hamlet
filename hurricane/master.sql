drop table if exists hurricane_irene

create table hurricane_irene as
select gid, serial_num, season, basin, sub_basin, name, iso_time, wmo_wind, wmo_pres, 'wmo_wind_%' as wmo_wind_radii , 
atc_rmw , atc_poci , atc_roci , atc_eye , atc_w34_r1 , atc_w34_r2 , atc_w34_r3 , atc_w34_r4 ,atc_w50_r1 ,
atc_w50_r2 , atc_w50_r3 ,atc_w50_r4 ,atc_w64_r1 ,atc_w64_r2, atc_w64_r3 , atc_w64_r4, geom from allstormspts_4326 where name = 'IRENE'

drop view if exists vw_quadrent_1 cascade;

drop view if exists vw_quadrent_2 cascade;

drop view if exists vw_quadrent_3 cascade;

drop view if exists vw_quadrent_4 cascade;

create or replace view vw_quadrent_1 as 
select gid, st_buffer(geom, (select distinct atc_w34_r1 from hurricane_irene where gid = '282105' ))::geometry(polygon,4326) as geom from hurricane_irene where gid = '282105';

create or replace view vw_quadrent_2 as
select gid, st_buffer(geom, (select distinct atc_w34_r2 from hurricane_irene limit 1))::geometry(polygon,4326) as geom from hurricane_irene limit 1;

create or replace view vw_quadrent_3 as
select gid, st_buffer(geom, (select distinct atc_w34_r3 from hurricane_irene limit 1))::geometry(polygon,4326) as geom from hurricane_irene limit 1;

create or replace view vw_quadrent_4 as
select gid, st_buffer(geom, (select distinct atc_w34_r4 from hurricane_irene limit 1))::geometry(polygon,4326) as geom from hurricane_irene limit 1;

create or replace view vw_quadrent_12 as
select a.gid, st_union(a.geom,b.geom) ::geometry(polygon,4326) as geom from vw_quadrent_1 as a, vw_quadrent_2 as b;

create or replace view vw_quadrent_34 as
select a.gid, st_union(a.geom,b.geom) ::geometry(polygon,4326) as geom from vw_quadrent_3 as a, vw_quadrent_4 as b;

create or replace view vw_quadrent_1234 as
select a.gid, st_union(a.geom,b.geom) ::geometry(polygon,4326) as geom from vw_quadrent_12 as a, vw_quadrent_34 as b;

--Start quadrent buffer---------
create or replace view vw_quadrent_1 as 
select ogc_food st_buffer(wkb_geometry, (select distinct atc_rmw from irene_49))::geometry(polygon,4326) 
as geom from irene_49;

create or replace view vw_quadrent_2 as
select ogc_fid, st_buffer(wkb_geometry, (select distinct atc_w34_r2 from irene_49))::geometry(polygon,4326) 
as wkb_geometry from irene_49 limit 1;

create or replace view vw_quadrent_3 as
select ogc_fid, st_buffer(wkb_geometry, (select distinct atc_w34_r3 from irene_49))::geometry(polygon,4326) 
as wkb_geometry from irene_49 limit 1;

create or replace view vw_quadrent_4 as
select ogc_fid, st_buffer(st_transform(wkb_geometry, 32612), (select distinct atc_w34_r4 from irene_49))::geometry(polygon,4326) 
as wkb_geometry from irene_49 limit 1;

create or replace view vw_quadrent_12 as
select st_union(a.wkb_geometry,b.wkb_geometry) ::geometry(polygon,4326) as wkb_geometry 
from vw_quadrent_1 as a, vw_quadrent_2 as b;

create or replace view vw_quadrent_34 as
select st_union(a.wkb_geometry,b.wkb_geometry) ::geometry(polygon,4326) as wkb_geometry 
from vw_quadrent_3 as a, vw_quadrent_4 as b;

create or replace view vw_quadrent_1234 as
select st_union(a.wkb_geometry,b.wkb_geometry) ::geometry(polygon,4326) as wkb_geometry 
from vw_quadrent_12 as a, vw_quadrent_34 as b;

create or replace view vw_quadrent_rmw_v5 as
select ogc_fid, st_buffer(st_transform(wkb_geometry,32612), (select distinct atc_rmw from irene_56)*1069)
as wkb_geometry from irene_45 limit 1;

--Max Wind Radius buffer------

create or replace view vw_quadrent_rmw_v6_4326 as 
select ogc_fid, st_transform(wkb_geometry, 4326)::geometry(polygon, 4326) as geom from vw_quadrent_rmw_v5

--radii of closed iso bar buffer---
create or replace view vw_quadrent_rmw_v7 as
select ogc_fid, st_transform(st_buffer(st_transform(wkb_geometry,32612), (select distinct atc_roci from irene_56)*1069),4326)
as wkb_geometry from irene_48 limit 1;


create table hurricane_katrina as
select gid, serial_num, season, basin, sub_basin, name, iso_time, wmo_wind, wmo_pres, 'wmo_wind_%' as wmo_wind_radii , 
atc_rmw , atc_poci , atc_roci , atc_eye , atc_w34_r1 , atc_w34_r2 , atc_w34_r3 , atc_w34_r4 ,atc_w50_r1 ,
atc_w50_r2 , atc_w50_r3 ,atc_w50_r4 ,atc_w64_r1 ,atc_w64_r2, atc_w64_r3 , atc_w64_r4, geom from allstormspts_4326 where name = 'KATRINA' and season = '2005'

 SELECT table_name
  FROM information_schema.tables
 WHERE table_schema='public'
   AND table_type='BASE TABLE';
   
create table hurricane_hugo as
select * from allstormspts_4326 where name = 'HUGO' and season = 1989

alter table hurricane_hugo add column id serial 


create or replace view vw_w34_r1
select iso_time, ogc_fid, st_transform(st_buffer(st_transform(wkb_geometry,32612), 
(select distinct atc_w34_r1 from {}_{})*1069),4326)::geometry(polygon, 4326) as geom 
from {}_{} limit 1;


select iso_time, ogc_fid, st_transform(st_buffer(st_transform(wkb_geometry,32612), 
(select distinct atc_w34_r2 from {}_{})*1069),4326)::geometry(polygon, 4326) as geom 
from {}_{} limit 1;


select iso_time, ogc_fid, st_transform(st_buffer(st_transform(wkb_geometry,32612), 
(select distinct atc_w34_r3 from {}_{})*1069),4326)::geometry(polygon, 4326) as geom 
from {}_{} limit 1;


select iso_time, ogc_fid, st_transform(st_buffer(st_transform(wkb_geometry,32612), 
(select distinct atc_w34_r4 from {}_{})*1069),4326)::geometry(polygon, 4326) as geom 
from {}_{} limit 1;



----------------------------------------------------------------------------------------------------------
select iso_time, ogc_fid, st_transform(st_buffer(st_transform(wkb_geometry,32612), 
(select distinct atc_w50_r1 from {}_{})*1069),4326)::geometry(polygon, 4326) as geom 
from {}_{} limit 1;


select iso_time, ogc_fid, st_transform(st_buffer(st_transform(wkb_geometry,32612), 
(select distinct atc_w50_r2 from {}_{})*1069),4326)::geometry(polygon, 4326) as geom 
from {}_{} limit 1;


select iso_time, ogc_fid, st_transform(st_buffer(st_transform(wkb_geometry,32612), 
(select distinct atc_w50_r3 from {}_{})*1069),4326)::geometry(polygon, 4326) as geom 
from {}_{} limit 1;


select iso_time, ogc_fid, st_transform(st_buffer(st_transform(wkb_geometry,32612), 
(select distinct atc_w50_r4 from {}_{})*1069),4326)::geometry(polygon, 4326) as geom 
from {}_{} limit 1;


-----------------------------------------------------------------------------------------------------------
select iso_time, ogc_fid, st_transform(st_buffer(st_transform(wkb_geometry,32612), 
(select distinct atc_w64_r1 from {}_{})*1069),4326)::geometry(polygon, 4326) as geom 
from {}_{} limit 1;


select iso_time, ogc_fid, st_transform(st_buffer(st_transform(wkb_geometry,32612), 
(select distinct atc_w64_r2 from {}_{})*1069),4326)::geometry(polygon, 4326) as geom 
from {}_{} limit 1;


select iso_time, ogc_fid, st_transform(st_buffer(st_transform(wkb_geometry,32612), 
(select distinct atc_w64_r3 from {}_{})*1069),4326)::geometry(polygon, 4326) as geom 
from {}_{} limit 1;


select iso_time, ogc_fid, st_transform(st_buffer(st_transform(wkb_geometry,32612), 
(select distinct atc_w64_r4 from {}_{})*1069),4326)::geometry(polygon, 4326) as geom 
from {}_{} limit 1;
