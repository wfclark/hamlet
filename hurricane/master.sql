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

