drop table acs2015_housing;
create table acs2015_housing(geoid character varying(50), countyfp character varying(50), price double precision)

create or replace view mhp_county as 
select a.gid, a.affgeoid, a.name, b.price, a.geom from county_4326_5m as a 
join acs2015_housing as b 
on a.affgeoid = b.geoid

select * from county_4326_5m