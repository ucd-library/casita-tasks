create schema if not exists roi;
set search_path=roi,public;

DO $$ BEGIN
create type goes_id as enum ('east','west');
create type instrument_id as enum ('abi');
create type packet_type_id as enum('metadata','data');
EXCEPTION
  WHEN duplicate_object then null;
    END $$;

DO $$ BEGIN
        create type abiResolution_id as enum ('500m','1km','2km','10km');
EXCEPTION
  WHEN duplicate_object then null;
          END $$;

create or replace view abiResolution(resolution,size) as
  (VALUES ('500m'::abiResolution_id,1),
  ('1km'::abiResolution_id,2),
  ('2km'::abiResolution_id,4),
  ('10km'::abiResolution_id,20));

create table if not exists abiBands (
  band integer primary key,
  wavelength float not null,
  region text,
  name text,
  resolution abiResolution_id
);

with bands( band,wavelength,region,name,resolution) as (
  values
  (1,0.47,'Visible','Blue','1km'),
  (2,0.64,'Visible','Red','500m'),
  (3,0.86,'Near-Infrared','Veggie','1km'),
  (4,1.37,'Near-Infrared','Cirrus','2km'),
  (5,1.6,'Near-Infrared','Snow/Ice','1km'),
  (6,2.2,'Near-Infrared','Cloud particle size','2km'),
  (7,3.9,'Infrared','Shortwave window','2km'),
  (8,6.2,'Infrared','Upper-level water vapor','2km'),
  (9,6.9,'Infrared','Midlevel water vapor','2km'),
  (10,7.3,'Infrared','Lower-level water vapor','2km'),
  (11,8.4,'Infrared','Cloud-top phase','2km'),
  (12,9.6,'Infrared','Ozone','2km'),
  (13,10.3,'Infrared','Clean longwave window','2km'),
  (14,11.2,'Infrared','Longwave window','2km'),
  (15,12.3,'Infrared','Dirty longwave window','2km'),
  (16,13.3,'Infrared','CO2 longwave','2km')
)
    insert into abibands (band,wavelength,region,name,resolution)
select b.band,b.wavelength,b.region,b.name,b.resolution::abiResolution_id
  from bands b
       left join abibands x using (band) where x is null;

create or replace function size (
  in band abibands,
  out integer)
  LANGUAGE SQL AS $$
  select r.size
  from abiresolution r
  where r.resolution = band.resolution
  $$;

create table if not exists abi (
  goes_id goes_id primary key,
  sat_longitude float,
  sat_height float,
  angle_ul float[2],
  angle_inc float,
  srid integer,
  proj4text text
);

with i(goes_id, sat_longitude, sat_height, angle_ul,angle_inc, srid, proj4text) as (
  values
  ('east'::goes_id, -75,35786023,'{-0.151872, 0.151872}'::float[2],14e-6,888887,'+proj=geos +x_0=0 +y_0=0 +lon_0=-75 +sweep=x +h=35786023 +ellps=GRS80 +datum=NAD83 +units=m +no_defs'),
  ('west'::goes_id,-137,35786023,'{-0.151872, 0.151872}'::float[2],14e-6,888897,'+proj=geos +x_0=0 +y_0=0 +lon_0=-137 +sweep=x +h=35786023 +ellps=GRS80 +datum=NAD83 +units=m +no_defs')
)
    insert into abi(goes_id, sat_longitude, sat_height, angle_ul,angle_inc, srid, proj4text)
select goes_id, i.sat_longitude, i.sat_height, i.angle_ul,i.angle_inc, i.srid, i.proj4text
  from i left join abi using (goes_id) where abi is null;

insert into spatial_ref_sys (srid,proj4text)
select a.srid,a.proj4text
  from abi a
       left join spatial_ref_sys s using (srid)
 where s is null;

DO $$
BEGIN
create type image_id as enum ('fulldisk','conus','mesoscale');
create type fixed_image_id as enum ('east-fulldisk','west-fulldisk','east-conus','west-conus');
EXCEPTION
  WHEN duplicate_object then null;
END
$$;

create table if not exists abi_fixed_image (
fixed_image_id fixed_image_id primary key,
goes_id goes_id,
image_id image_id,
angles box2d,
rc_box box2d,
width_height integer[2],
bbox geometry('Polygon')
);

with a(goes_id,name,angles) as
(values ('east','fulldisk','BOX(-0.151872 -0.151872,0.151872 0.151872)'::BOX2D),
         ('east','conus','BOX(-0.101360 0.044240, 0.038640 0.128240)'::BOX2D),
         ('west','fulldisk','BOX(-0.151872 -0.151872,0.151872 0.151872)'::BOX2D),
         ('west','conus','BOX(-0.070000 0.044240, 0.070000 0.128240)'::BOX2D)
)
insert into abi_fixed_image (fixed_image_id,goes_id,image_id,angles,bbox,rc_box,width_height)
select
(a.goes_id||'-'||a.name)::fixed_image_id as fixed_image_id,
a.goes_id::goes_id,
a.name::image_id,
a.angles,
st_setsrid(
st_makebox2d(st_makepoint(sat_height*st_xmin(a.angles),sat_height*st_ymin(a.angles)),
             st_makepoint(sat_height*st_xmax(a.angles),sat_height*st_ymax(a.angles)))
,srid) as bbox,
st_makebox2d(
 st_makepoint((st_xmin(a.angles)-abi.angle_ul[1])/abi.angle_inc,-(st_ymax(a.angles)-abi.angle_ul[2])/abi.angle_inc),
 st_makepoint((st_xmax(a.angles)-abi.angle_ul[1])/abi.angle_inc,-(st_ymin(a.angles)-abi.angle_ul[2])/abi.angle_inc)) as rc_box,
ARRAY[(st_xmax(a.angles)-st_xmin(a.angles))/abi.angle_inc,(st_ymax(a.angles)-st_ymin(a.angles))/abi.angle_inc]::integer[2] as width_height
from abi join
a on abi.goes_id=a.goes_id::goes_id
  left join abi_fixed_image f
      on (a.goes_id||'-'||a.name)::fixed_image_id=f.fixed_image_id
 where f is null;

create table if not exists abi_fixed_image_block (
  fixed_image_block_id text primary key,
  fixed_image_id fixed_image_id,
  box box2d
);

with i(fixed_image_block_id,fixed_image_id,box) as (
  VALUES
('west-fulldisk-7232-0','west-fulldisk'::fixed_image_id,'BOX(7232 0,9040 604)'::box2d),
('west-fulldisk-9040-0','west-fulldisk'::fixed_image_id,'BOX(9040 0,10848 604)'::box2d),
('west-fulldisk-10848-0','west-fulldisk'::fixed_image_id,'BOX(10848 0,12656 604)'::box2d),
('west-fulldisk-12656-0','west-fulldisk'::fixed_image_id,'BOX(12656 0,14464 604)'::box2d),
('west-fulldisk-3616-604','west-fulldisk'::fixed_image_id,'BOX(3616 604,5424 1616)'::box2d),
('west-fulldisk-5424-604','west-fulldisk'::fixed_image_id,'BOX(5424 604,7232 1616)'::box2d),
('west-fulldisk-7232-604','west-fulldisk'::fixed_image_id,'BOX(7232 604,9040 1616)'::box2d),
('west-fulldisk-9040-604','west-fulldisk'::fixed_image_id,'BOX(9040 604,10848 1616)'::box2d),
('west-fulldisk-10848-604','west-fulldisk'::fixed_image_id,'BOX(10848 604,12656 1616)'::box2d),
('west-fulldisk-12656-604','west-fulldisk'::fixed_image_id,'BOX(12656 604,14464 1616)'::box2d),
('west-fulldisk-14464-604','west-fulldisk'::fixed_image_id,'BOX(14464 604,16272 1616)'::box2d),
('west-fulldisk-16272-604','west-fulldisk'::fixed_image_id,'BOX(16272 604,18080 1616)'::box2d),
('west-fulldisk-3616-1616','west-fulldisk'::fixed_image_id,'BOX(3616 1616,5424 2628)'::box2d),
('west-fulldisk-5424-1616','west-fulldisk'::fixed_image_id,'BOX(5424 1616,7232 2628)'::box2d),
('west-fulldisk-7232-1616','west-fulldisk'::fixed_image_id,'BOX(7232 1616,9040 2628)'::box2d),
('west-fulldisk-9040-1616','west-fulldisk'::fixed_image_id,'BOX(9040 1616,10848 2628)'::box2d),
('west-fulldisk-10848-1616','west-fulldisk'::fixed_image_id,'BOX(10848 1616,12656 2628)'::box2d),
('west-fulldisk-12656-1616','west-fulldisk'::fixed_image_id,'BOX(12656 1616,14464 2628)'::box2d),
('west-fulldisk-14464-1616','west-fulldisk'::fixed_image_id,'BOX(14464 1616,16272 2628)'::box2d),
('west-fulldisk-16272-1616','west-fulldisk'::fixed_image_id,'BOX(16272 1616,18080 2628)'::box2d),
('west-fulldisk-1808-2628','west-fulldisk'::fixed_image_id,'BOX(1808 2628,3616 3640)'::box2d),
('west-fulldisk-3616-2628','west-fulldisk'::fixed_image_id,'BOX(3616 2628,5424 3640)'::box2d),
('west-fulldisk-5424-2628','west-fulldisk'::fixed_image_id,'BOX(5424 2628,7232 3640)'::box2d),
('west-fulldisk-7232-2628','west-fulldisk'::fixed_image_id,'BOX(7232 2628,9040 3640)'::box2d),
('west-fulldisk-9040-2628','west-fulldisk'::fixed_image_id,'BOX(9040 2628,10848 3640)'::box2d),
('west-fulldisk-10848-2628','west-fulldisk'::fixed_image_id,'BOX(10848 2628,12656 3640)'::box2d),
('west-fulldisk-12656-2628','west-fulldisk'::fixed_image_id,'BOX(12656 2628,14464 3640)'::box2d),
('west-fulldisk-14464-2628','west-fulldisk'::fixed_image_id,'BOX(14464 2628,16272 3640)'::box2d),
('west-fulldisk-16272-2628','west-fulldisk'::fixed_image_id,'BOX(16272 2628,18080 3640)'::box2d),
('west-fulldisk-18080-2628','west-fulldisk'::fixed_image_id,'BOX(18080 2628,19888 3640)'::box2d),
('west-fulldisk-1808-3640','west-fulldisk'::fixed_image_id,'BOX(1808 3640,3616 4652)'::box2d),
('west-fulldisk-3616-3640','west-fulldisk'::fixed_image_id,'BOX(3616 3640,5424 4652)'::box2d),
('west-fulldisk-5424-3640','west-fulldisk'::fixed_image_id,'BOX(5424 3640,7232 4652)'::box2d),
('west-fulldisk-7232-3640','west-fulldisk'::fixed_image_id,'BOX(7232 3640,9040 4652)'::box2d),
('west-fulldisk-9040-3640','west-fulldisk'::fixed_image_id,'BOX(9040 3640,10848 4652)'::box2d),
('west-fulldisk-10848-3640','west-fulldisk'::fixed_image_id,'BOX(10848 3640,12656 4652)'::box2d),
('west-fulldisk-12656-3640','west-fulldisk'::fixed_image_id,'BOX(12656 3640,14464 4652)'::box2d),
('west-fulldisk-14464-3640','west-fulldisk'::fixed_image_id,'BOX(14464 3640,16272 4652)'::box2d),
('west-fulldisk-16272-3640','west-fulldisk'::fixed_image_id,'BOX(16272 3640,18080 4652)'::box2d),
('west-fulldisk-18080-3640','west-fulldisk'::fixed_image_id,'BOX(18080 3640,19888 4652)'::box2d),
('west-fulldisk-0-4652','west-fulldisk'::fixed_image_id,'BOX(0 4652,1808 5664)'::box2d),
('west-fulldisk-1808-4652','west-fulldisk'::fixed_image_id,'BOX(1808 4652,3616 5664)'::box2d),
('west-fulldisk-3616-4652','west-fulldisk'::fixed_image_id,'BOX(3616 4652,5424 5664)'::box2d),
('west-fulldisk-5424-4652','west-fulldisk'::fixed_image_id,'BOX(5424 4652,7232 5664)'::box2d),
('west-fulldisk-7232-4652','west-fulldisk'::fixed_image_id,'BOX(7232 4652,9040 5664)'::box2d),
('west-fulldisk-9040-4652','west-fulldisk'::fixed_image_id,'BOX(9040 4652,10848 5664)'::box2d),
('west-fulldisk-10848-4652','west-fulldisk'::fixed_image_id,'BOX(10848 4652,12656 5664)'::box2d),
('west-fulldisk-12656-4652','west-fulldisk'::fixed_image_id,'BOX(12656 4652,14464 5664)'::box2d),
('west-fulldisk-14464-4652','west-fulldisk'::fixed_image_id,'BOX(14464 4652,16272 5664)'::box2d),
('west-fulldisk-16272-4652','west-fulldisk'::fixed_image_id,'BOX(16272 4652,18080 5664)'::box2d),
('west-fulldisk-18080-4652','west-fulldisk'::fixed_image_id,'BOX(18080 4652,19888 5664)'::box2d),
('west-fulldisk-19888-4652','west-fulldisk'::fixed_image_id,'BOX(19888 4652,21696 5664)'::box2d),
('west-fulldisk-0-5664','west-fulldisk'::fixed_image_id,'BOX(0 5664,1808 6676)'::box2d),
('west-fulldisk-1808-5664','west-fulldisk'::fixed_image_id,'BOX(1808 5664,3616 6676)'::box2d),
('west-fulldisk-3616-5664','west-fulldisk'::fixed_image_id,'BOX(3616 5664,5424 6676)'::box2d),
('west-fulldisk-5424-5664','west-fulldisk'::fixed_image_id,'BOX(5424 5664,7232 6676)'::box2d),
('west-fulldisk-7232-5664','west-fulldisk'::fixed_image_id,'BOX(7232 5664,9040 6676)'::box2d),
('west-fulldisk-9040-5664','west-fulldisk'::fixed_image_id,'BOX(9040 5664,10848 6676)'::box2d),
('west-fulldisk-10848-5664','west-fulldisk'::fixed_image_id,'BOX(10848 5664,12656 6676)'::box2d),
('west-fulldisk-12656-5664','west-fulldisk'::fixed_image_id,'BOX(12656 5664,14464 6676)'::box2d),
('west-fulldisk-14464-5664','west-fulldisk'::fixed_image_id,'BOX(14464 5664,16272 6676)'::box2d),
('west-fulldisk-16272-5664','west-fulldisk'::fixed_image_id,'BOX(16272 5664,18080 6676)'::box2d),
('west-fulldisk-18080-5664','west-fulldisk'::fixed_image_id,'BOX(18080 5664,19888 6676)'::box2d),
('west-fulldisk-19888-5664','west-fulldisk'::fixed_image_id,'BOX(19888 5664,21696 6676)'::box2d),
('west-fulldisk-0-6676','west-fulldisk'::fixed_image_id,'BOX(0 6676,1808 7688)'::box2d),
('west-fulldisk-1808-6676','west-fulldisk'::fixed_image_id,'BOX(1808 6676,3616 7688)'::box2d),
('west-fulldisk-3616-6676','west-fulldisk'::fixed_image_id,'BOX(3616 6676,5424 7688)'::box2d),
('west-fulldisk-5424-6676','west-fulldisk'::fixed_image_id,'BOX(5424 6676,7232 7688)'::box2d),
('west-fulldisk-7232-6676','west-fulldisk'::fixed_image_id,'BOX(7232 6676,9040 7688)'::box2d),
('west-fulldisk-9040-6676','west-fulldisk'::fixed_image_id,'BOX(9040 6676,10848 7688)'::box2d),
('west-fulldisk-10848-6676','west-fulldisk'::fixed_image_id,'BOX(10848 6676,12656 7688)'::box2d),
('west-fulldisk-12656-6676','west-fulldisk'::fixed_image_id,'BOX(12656 6676,14464 7688)'::box2d),
('west-fulldisk-14464-6676','west-fulldisk'::fixed_image_id,'BOX(14464 6676,16272 7688)'::box2d),
('west-fulldisk-16272-6676','west-fulldisk'::fixed_image_id,'BOX(16272 6676,18080 7688)'::box2d),
('west-fulldisk-18080-6676','west-fulldisk'::fixed_image_id,'BOX(18080 6676,19888 7688)'::box2d),
('west-fulldisk-19888-6676','west-fulldisk'::fixed_image_id,'BOX(19888 6676,21696 7688)'::box2d),
('west-fulldisk-0-7688','west-fulldisk'::fixed_image_id,'BOX(0 7688,1808 8700)'::box2d),
('west-fulldisk-1808-7688','west-fulldisk'::fixed_image_id,'BOX(1808 7688,3616 8700)'::box2d),
('west-fulldisk-3616-7688','west-fulldisk'::fixed_image_id,'BOX(3616 7688,5424 8700)'::box2d),
('west-fulldisk-5424-7688','west-fulldisk'::fixed_image_id,'BOX(5424 7688,7232 8700)'::box2d),
('west-fulldisk-7232-7688','west-fulldisk'::fixed_image_id,'BOX(7232 7688,9040 8700)'::box2d),
('west-fulldisk-9040-7688','west-fulldisk'::fixed_image_id,'BOX(9040 7688,10848 8700)'::box2d),
('west-fulldisk-10848-7688','west-fulldisk'::fixed_image_id,'BOX(10848 7688,12656 8700)'::box2d),
('west-fulldisk-12656-7688','west-fulldisk'::fixed_image_id,'BOX(12656 7688,14464 8700)'::box2d),
('west-fulldisk-14464-7688','west-fulldisk'::fixed_image_id,'BOX(14464 7688,16272 8700)'::box2d),
('west-fulldisk-16272-7688','west-fulldisk'::fixed_image_id,'BOX(16272 7688,18080 8700)'::box2d),
('west-fulldisk-18080-7688','west-fulldisk'::fixed_image_id,'BOX(18080 7688,19888 8700)'::box2d),
('west-fulldisk-19888-7688','west-fulldisk'::fixed_image_id,'BOX(19888 7688,21696 8700)'::box2d),
('west-fulldisk-0-8700','west-fulldisk'::fixed_image_id,'BOX(0 8700,1808 9712)'::box2d),
('west-fulldisk-1808-8700','west-fulldisk'::fixed_image_id,'BOX(1808 8700,3616 9712)'::box2d),
('west-fulldisk-3616-8700','west-fulldisk'::fixed_image_id,'BOX(3616 8700,5424 9712)'::box2d),
('west-fulldisk-5424-8700','west-fulldisk'::fixed_image_id,'BOX(5424 8700,7232 9712)'::box2d),
('west-fulldisk-7232-8700','west-fulldisk'::fixed_image_id,'BOX(7232 8700,9040 9712)'::box2d),
('west-fulldisk-9040-8700','west-fulldisk'::fixed_image_id,'BOX(9040 8700,10848 9712)'::box2d),
('west-fulldisk-10848-8700','west-fulldisk'::fixed_image_id,'BOX(10848 8700,12656 9712)'::box2d),
('west-fulldisk-12656-8700','west-fulldisk'::fixed_image_id,'BOX(12656 8700,14464 9712)'::box2d),
('west-fulldisk-14464-8700','west-fulldisk'::fixed_image_id,'BOX(14464 8700,16272 9712)'::box2d),
('west-fulldisk-16272-8700','west-fulldisk'::fixed_image_id,'BOX(16272 8700,18080 9712)'::box2d),
('west-fulldisk-18080-8700','west-fulldisk'::fixed_image_id,'BOX(18080 8700,19888 9712)'::box2d),
('west-fulldisk-19888-8700','west-fulldisk'::fixed_image_id,'BOX(19888 8700,21696 9712)'::box2d),
('west-fulldisk-0-9712','west-fulldisk'::fixed_image_id,'BOX(0 9712,1808 10724)'::box2d),
('west-fulldisk-1808-9712','west-fulldisk'::fixed_image_id,'BOX(1808 9712,3616 10724)'::box2d),
('west-fulldisk-3616-9712','west-fulldisk'::fixed_image_id,'BOX(3616 9712,5424 10724)'::box2d),
('west-fulldisk-5424-9712','west-fulldisk'::fixed_image_id,'BOX(5424 9712,7232 10724)'::box2d),
('west-fulldisk-7232-9712','west-fulldisk'::fixed_image_id,'BOX(7232 9712,9040 10724)'::box2d),
('west-fulldisk-9040-9712','west-fulldisk'::fixed_image_id,'BOX(9040 9712,10848 10724)'::box2d),
('west-fulldisk-10848-9712','west-fulldisk'::fixed_image_id,'BOX(10848 9712,12656 10724)'::box2d),
('west-fulldisk-12656-9712','west-fulldisk'::fixed_image_id,'BOX(12656 9712,14464 10724)'::box2d),
('west-fulldisk-14464-9712','west-fulldisk'::fixed_image_id,'BOX(14464 9712,16272 10724)'::box2d),
('west-fulldisk-16272-9712','west-fulldisk'::fixed_image_id,'BOX(16272 9712,18080 10724)'::box2d),
('west-fulldisk-18080-9712','west-fulldisk'::fixed_image_id,'BOX(18080 9712,19888 10724)'::box2d),
('west-fulldisk-19888-9712','west-fulldisk'::fixed_image_id,'BOX(19888 9712,21696 10724)'::box2d),
('west-fulldisk-0-10724','west-fulldisk'::fixed_image_id,'BOX(0 10724,1808 11732)'::box2d),
('west-fulldisk-1808-10724','west-fulldisk'::fixed_image_id,'BOX(1808 10724,3616 11732)'::box2d),
('west-fulldisk-3616-10724','west-fulldisk'::fixed_image_id,'BOX(3616 10724,5424 11732)'::box2d),
('west-fulldisk-5424-10724','west-fulldisk'::fixed_image_id,'BOX(5424 10724,7232 11732)'::box2d),
('west-fulldisk-7232-10724','west-fulldisk'::fixed_image_id,'BOX(7232 10724,9040 11732)'::box2d),
('west-fulldisk-9040-10724','west-fulldisk'::fixed_image_id,'BOX(9040 10724,10848 11732)'::box2d),
('west-fulldisk-10848-10724','west-fulldisk'::fixed_image_id,'BOX(10848 10724,12656 11732)'::box2d),
('west-fulldisk-12656-10724','west-fulldisk'::fixed_image_id,'BOX(12656 10724,14464 11732)'::box2d),
('west-fulldisk-14464-10724','west-fulldisk'::fixed_image_id,'BOX(14464 10724,16272 11732)'::box2d),
('west-fulldisk-16272-10724','west-fulldisk'::fixed_image_id,'BOX(16272 10724,18080 11732)'::box2d),
('west-fulldisk-18080-10724','west-fulldisk'::fixed_image_id,'BOX(18080 10724,19888 11732)'::box2d),
('west-fulldisk-19888-10724','west-fulldisk'::fixed_image_id,'BOX(19888 10724,21696 11732)'::box2d),
('west-fulldisk-0-11732','west-fulldisk'::fixed_image_id,'BOX(0 11732,1808 12744)'::box2d),
('west-fulldisk-1808-11732','west-fulldisk'::fixed_image_id,'BOX(1808 11732,3616 12744)'::box2d),
('west-fulldisk-3616-11732','west-fulldisk'::fixed_image_id,'BOX(3616 11732,5424 12744)'::box2d),
('west-fulldisk-5424-11732','west-fulldisk'::fixed_image_id,'BOX(5424 11732,7232 12744)'::box2d),
('west-fulldisk-7232-11732','west-fulldisk'::fixed_image_id,'BOX(7232 11732,9040 12744)'::box2d),
('west-fulldisk-9040-11732','west-fulldisk'::fixed_image_id,'BOX(9040 11732,10848 12744)'::box2d),
('west-fulldisk-10848-11732','west-fulldisk'::fixed_image_id,'BOX(10848 11732,12656 12744)'::box2d),
('west-fulldisk-12656-11732','west-fulldisk'::fixed_image_id,'BOX(12656 11732,14464 12744)'::box2d),
('west-fulldisk-14464-11732','west-fulldisk'::fixed_image_id,'BOX(14464 11732,16272 12744)'::box2d),
('west-fulldisk-16272-11732','west-fulldisk'::fixed_image_id,'BOX(16272 11732,18080 12744)'::box2d),
('west-fulldisk-18080-11732','west-fulldisk'::fixed_image_id,'BOX(18080 11732,19888 12744)'::box2d),
('west-fulldisk-19888-11732','west-fulldisk'::fixed_image_id,'BOX(19888 11732,21696 12744)'::box2d),
('west-fulldisk-0-12744','west-fulldisk'::fixed_image_id,'BOX(0 12744,1808 13756)'::box2d),
('west-fulldisk-1808-12744','west-fulldisk'::fixed_image_id,'BOX(1808 12744,3616 13756)'::box2d),
('west-fulldisk-3616-12744','west-fulldisk'::fixed_image_id,'BOX(3616 12744,5424 13756)'::box2d),
('west-fulldisk-5424-12744','west-fulldisk'::fixed_image_id,'BOX(5424 12744,7232 13756)'::box2d),
('west-fulldisk-7232-12744','west-fulldisk'::fixed_image_id,'BOX(7232 12744,9040 13756)'::box2d),
('west-fulldisk-9040-12744','west-fulldisk'::fixed_image_id,'BOX(9040 12744,10848 13756)'::box2d),
('west-fulldisk-10848-12744','west-fulldisk'::fixed_image_id,'BOX(10848 12744,12656 13756)'::box2d),
('west-fulldisk-12656-12744','west-fulldisk'::fixed_image_id,'BOX(12656 12744,14464 13756)'::box2d),
('west-fulldisk-14464-12744','west-fulldisk'::fixed_image_id,'BOX(14464 12744,16272 13756)'::box2d),
('west-fulldisk-16272-12744','west-fulldisk'::fixed_image_id,'BOX(16272 12744,18080 13756)'::box2d),
('west-fulldisk-18080-12744','west-fulldisk'::fixed_image_id,'BOX(18080 12744,19888 13756)'::box2d),
('west-fulldisk-19888-12744','west-fulldisk'::fixed_image_id,'BOX(19888 12744,21696 13756)'::box2d),
('west-fulldisk-0-13756','west-fulldisk'::fixed_image_id,'BOX(0 13756,1808 14768)'::box2d),
('west-fulldisk-1808-13756','west-fulldisk'::fixed_image_id,'BOX(1808 13756,3616 14768)'::box2d),
('west-fulldisk-3616-13756','west-fulldisk'::fixed_image_id,'BOX(3616 13756,5424 14768)'::box2d),
('west-fulldisk-5424-13756','west-fulldisk'::fixed_image_id,'BOX(5424 13756,7232 14768)'::box2d),
('west-fulldisk-7232-13756','west-fulldisk'::fixed_image_id,'BOX(7232 13756,9040 14768)'::box2d),
('west-fulldisk-9040-13756','west-fulldisk'::fixed_image_id,'BOX(9040 13756,10848 14768)'::box2d),
('west-fulldisk-10848-13756','west-fulldisk'::fixed_image_id,'BOX(10848 13756,12656 14768)'::box2d),
('west-fulldisk-12656-13756','west-fulldisk'::fixed_image_id,'BOX(12656 13756,14464 14768)'::box2d),
('west-fulldisk-14464-13756','west-fulldisk'::fixed_image_id,'BOX(14464 13756,16272 14768)'::box2d),
('west-fulldisk-16272-13756','west-fulldisk'::fixed_image_id,'BOX(16272 13756,18080 14768)'::box2d),
('west-fulldisk-18080-13756','west-fulldisk'::fixed_image_id,'BOX(18080 13756,19888 14768)'::box2d),
('west-fulldisk-19888-13756','west-fulldisk'::fixed_image_id,'BOX(19888 13756,21696 14768)'::box2d),
('west-fulldisk-0-14768','west-fulldisk'::fixed_image_id,'BOX(0 14768,1808 15780)'::box2d),
('west-fulldisk-1808-14768','west-fulldisk'::fixed_image_id,'BOX(1808 14768,3616 15780)'::box2d),
('west-fulldisk-3616-14768','west-fulldisk'::fixed_image_id,'BOX(3616 14768,5424 15780)'::box2d),
('west-fulldisk-5424-14768','west-fulldisk'::fixed_image_id,'BOX(5424 14768,7232 15780)'::box2d),
('west-fulldisk-7232-14768','west-fulldisk'::fixed_image_id,'BOX(7232 14768,9040 15780)'::box2d),
('west-fulldisk-9040-14768','west-fulldisk'::fixed_image_id,'BOX(9040 14768,10848 15780)'::box2d),
('west-fulldisk-10848-14768','west-fulldisk'::fixed_image_id,'BOX(10848 14768,12656 15780)'::box2d),
('west-fulldisk-12656-14768','west-fulldisk'::fixed_image_id,'BOX(12656 14768,14464 15780)'::box2d),
('west-fulldisk-14464-14768','west-fulldisk'::fixed_image_id,'BOX(14464 14768,16272 15780)'::box2d),
('west-fulldisk-16272-14768','west-fulldisk'::fixed_image_id,'BOX(16272 14768,18080 15780)'::box2d),
('west-fulldisk-18080-14768','west-fulldisk'::fixed_image_id,'BOX(18080 14768,19888 15780)'::box2d),
('west-fulldisk-19888-14768','west-fulldisk'::fixed_image_id,'BOX(19888 14768,21696 15780)'::box2d),
('west-fulldisk-0-15780','west-fulldisk'::fixed_image_id,'BOX(0 15780,1808 16792)'::box2d),
('west-fulldisk-1808-15780','west-fulldisk'::fixed_image_id,'BOX(1808 15780,3616 16792)'::box2d),
('west-fulldisk-3616-15780','west-fulldisk'::fixed_image_id,'BOX(3616 15780,5424 16792)'::box2d),
('west-fulldisk-5424-15780','west-fulldisk'::fixed_image_id,'BOX(5424 15780,7232 16792)'::box2d),
('west-fulldisk-7232-15780','west-fulldisk'::fixed_image_id,'BOX(7232 15780,9040 16792)'::box2d),
('west-fulldisk-9040-15780','west-fulldisk'::fixed_image_id,'BOX(9040 15780,10848 16792)'::box2d),
('west-fulldisk-10848-15780','west-fulldisk'::fixed_image_id,'BOX(10848 15780,12656 16792)'::box2d),
('west-fulldisk-12656-15780','west-fulldisk'::fixed_image_id,'BOX(12656 15780,14464 16792)'::box2d),
('west-fulldisk-14464-15780','west-fulldisk'::fixed_image_id,'BOX(14464 15780,16272 16792)'::box2d),
('west-fulldisk-16272-15780','west-fulldisk'::fixed_image_id,'BOX(16272 15780,18080 16792)'::box2d),
('west-fulldisk-18080-15780','west-fulldisk'::fixed_image_id,'BOX(18080 15780,19888 16792)'::box2d),
('west-fulldisk-19888-15780','west-fulldisk'::fixed_image_id,'BOX(19888 15780,21696 16792)'::box2d),
('west-fulldisk-0-16792','west-fulldisk'::fixed_image_id,'BOX(0 16792,1808 17804)'::box2d),
('west-fulldisk-1808-16792','west-fulldisk'::fixed_image_id,'BOX(1808 16792,3616 17804)'::box2d),
('west-fulldisk-3616-16792','west-fulldisk'::fixed_image_id,'BOX(3616 16792,5424 17804)'::box2d),
('west-fulldisk-5424-16792','west-fulldisk'::fixed_image_id,'BOX(5424 16792,7232 17804)'::box2d),
('west-fulldisk-7232-16792','west-fulldisk'::fixed_image_id,'BOX(7232 16792,9040 17804)'::box2d),
('west-fulldisk-9040-16792','west-fulldisk'::fixed_image_id,'BOX(9040 16792,10848 17804)'::box2d),
('west-fulldisk-10848-16792','west-fulldisk'::fixed_image_id,'BOX(10848 16792,12656 17804)'::box2d),
('west-fulldisk-12656-16792','west-fulldisk'::fixed_image_id,'BOX(12656 16792,14464 17804)'::box2d),
('west-fulldisk-14464-16792','west-fulldisk'::fixed_image_id,'BOX(14464 16792,16272 17804)'::box2d),
('west-fulldisk-16272-16792','west-fulldisk'::fixed_image_id,'BOX(16272 16792,18080 17804)'::box2d),
('west-fulldisk-18080-16792','west-fulldisk'::fixed_image_id,'BOX(18080 16792,19888 17804)'::box2d),
('west-fulldisk-19888-16792','west-fulldisk'::fixed_image_id,'BOX(19888 16792,21696 17804)'::box2d),
('west-fulldisk-1808-17804','west-fulldisk'::fixed_image_id,'BOX(1808 17804,3616 18816)'::box2d),
('west-fulldisk-3616-17804','west-fulldisk'::fixed_image_id,'BOX(3616 17804,5424 18816)'::box2d),
('west-fulldisk-5424-17804','west-fulldisk'::fixed_image_id,'BOX(5424 17804,7232 18816)'::box2d),
('west-fulldisk-7232-17804','west-fulldisk'::fixed_image_id,'BOX(7232 17804,9040 18816)'::box2d),
('west-fulldisk-9040-17804','west-fulldisk'::fixed_image_id,'BOX(9040 17804,10848 18816)'::box2d),
('west-fulldisk-10848-17804','west-fulldisk'::fixed_image_id,'BOX(10848 17804,12656 18816)'::box2d),
('west-fulldisk-12656-17804','west-fulldisk'::fixed_image_id,'BOX(12656 17804,14464 18816)'::box2d),
('west-fulldisk-14464-17804','west-fulldisk'::fixed_image_id,'BOX(14464 17804,16272 18816)'::box2d),
('west-fulldisk-16272-17804','west-fulldisk'::fixed_image_id,'BOX(16272 17804,18080 18816)'::box2d),
('west-fulldisk-18080-17804','west-fulldisk'::fixed_image_id,'BOX(18080 17804,19888 18816)'::box2d),
('west-fulldisk-1808-18816','west-fulldisk'::fixed_image_id,'BOX(1808 18816,3616 19828)'::box2d),
('west-fulldisk-3616-18816','west-fulldisk'::fixed_image_id,'BOX(3616 18816,5424 19828)'::box2d),
('west-fulldisk-5424-18816','west-fulldisk'::fixed_image_id,'BOX(5424 18816,7232 19828)'::box2d),
('west-fulldisk-7232-18816','west-fulldisk'::fixed_image_id,'BOX(7232 18816,9040 19828)'::box2d),
('west-fulldisk-9040-18816','west-fulldisk'::fixed_image_id,'BOX(9040 18816,10848 19828)'::box2d),
('west-fulldisk-10848-18816','west-fulldisk'::fixed_image_id,'BOX(10848 18816,12656 19828)'::box2d),
('west-fulldisk-12656-18816','west-fulldisk'::fixed_image_id,'BOX(12656 18816,14464 19828)'::box2d),
('west-fulldisk-14464-18816','west-fulldisk'::fixed_image_id,'BOX(14464 18816,16272 19828)'::box2d),
('west-fulldisk-16272-18816','west-fulldisk'::fixed_image_id,'BOX(16272 18816,18080 19828)'::box2d),
('west-fulldisk-18080-18816','west-fulldisk'::fixed_image_id,'BOX(18080 18816,19888 19828)'::box2d),
('west-fulldisk-3616-19828','west-fulldisk'::fixed_image_id,'BOX(3616 19828,5424 20840)'::box2d),
('west-fulldisk-5424-19828','west-fulldisk'::fixed_image_id,'BOX(5424 19828,7232 20840)'::box2d),
('west-fulldisk-7232-19828','west-fulldisk'::fixed_image_id,'BOX(7232 19828,9040 20840)'::box2d),
('west-fulldisk-9040-19828','west-fulldisk'::fixed_image_id,'BOX(9040 19828,10848 20840)'::box2d),
('west-fulldisk-10848-19828','west-fulldisk'::fixed_image_id,'BOX(10848 19828,12656 20840)'::box2d),
('west-fulldisk-12656-19828','west-fulldisk'::fixed_image_id,'BOX(12656 19828,14464 20840)'::box2d),
('west-fulldisk-14464-19828','west-fulldisk'::fixed_image_id,'BOX(14464 19828,16272 20840)'::box2d),
('west-fulldisk-16272-19828','west-fulldisk'::fixed_image_id,'BOX(16272 19828,18080 20840)'::box2d),
('west-fulldisk-5424-20840','west-fulldisk'::fixed_image_id,'BOX(5424 20840,7232 21696)'::box2d),
('west-fulldisk-7232-20840','west-fulldisk'::fixed_image_id,'BOX(7232 20840,9040 21696)'::box2d),
('west-fulldisk-9040-20840','west-fulldisk'::fixed_image_id,'BOX(9040 20840,10848 21696)'::box2d),
('west-fulldisk-10848-20840','west-fulldisk'::fixed_image_id,'BOX(10848 20840,12656 21696)'::box2d),
('west-fulldisk-12656-20840','west-fulldisk'::fixed_image_id,'BOX(12656 20840,14464 21696)'::box2d),
('west-fulldisk-14464-20840','west-fulldisk'::fixed_image_id,'BOX(14464 20840,16272 21696)'::box2d),
('west-conus-0-0','west-conus'::fixed_image_id,'BOX(0 0,1668 852)'::box2d),
('west-conus-1668-0','west-conus'::fixed_image_id,'BOX(1668 0,3332 852)'::box2d),
('west-conus-3332-0','west-conus'::fixed_image_id,'BOX(3332 0,5000 852)'::box2d),
('west-conus-5000-0','west-conus'::fixed_image_id,'BOX(5000 0,6664 852)'::box2d),
('west-conus-6664-0','west-conus'::fixed_image_id,'BOX(6664 0,8332 852)'::box2d),
('west-conus-8332-0','west-conus'::fixed_image_id,'BOX(8332 0,10000 852)'::box2d),
('west-conus-0-852','west-conus'::fixed_image_id,'BOX(0 852,1668 1860)'::box2d),
('west-conus-1668-852','west-conus'::fixed_image_id,'BOX(1668 852,3332 1860)'::box2d),
('west-conus-3332-852','west-conus'::fixed_image_id,'BOX(3332 852,5000 1860)'::box2d),
('west-conus-5000-852','west-conus'::fixed_image_id,'BOX(5000 852,6664 1860)'::box2d),
('west-conus-6664-852','west-conus'::fixed_image_id,'BOX(6664 852,8332 1860)'::box2d),
('west-conus-8332-852','west-conus'::fixed_image_id,'BOX(8332 852,10000 1860)'::box2d),
('west-conus-0-1860','west-conus'::fixed_image_id,'BOX(0 1860,1668 2872)'::box2d),
('west-conus-1668-1860','west-conus'::fixed_image_id,'BOX(1668 1860,3332 2872)'::box2d),
('west-conus-3332-1860','west-conus'::fixed_image_id,'BOX(3332 1860,5000 2872)'::box2d),
('west-conus-5000-1860','west-conus'::fixed_image_id,'BOX(5000 1860,6664 2872)'::box2d),
('west-conus-6664-1860','west-conus'::fixed_image_id,'BOX(6664 1860,8332 2872)'::box2d),
('west-conus-8332-1860','west-conus'::fixed_image_id,'BOX(8332 1860,10000 2872)'::box2d),
('west-conus-0-2872','west-conus'::fixed_image_id,'BOX(0 2872,1668 3884)'::box2d),
('west-conus-1668-2872','west-conus'::fixed_image_id,'BOX(1668 2872,3332 3884)'::box2d),
('west-conus-3332-2872','west-conus'::fixed_image_id,'BOX(3332 2872,5000 3884)'::box2d),
('west-conus-5000-2872','west-conus'::fixed_image_id,'BOX(5000 2872,6664 3884)'::box2d),
('west-conus-6664-2872','west-conus'::fixed_image_id,'BOX(6664 2872,8332 3884)'::box2d),
('west-conus-8332-2872','west-conus'::fixed_image_id,'BOX(8332 2872,10000 3884)'::box2d),
('west-conus-0-3884','west-conus'::fixed_image_id,'BOX(0 3884,1668 4896)'::box2d),
('west-conus-1668-3884','west-conus'::fixed_image_id,'BOX(1668 3884,3332 4896)'::box2d),
('west-conus-3332-3884','west-conus'::fixed_image_id,'BOX(3332 3884,5000 4896)'::box2d),
('west-conus-5000-3884','west-conus'::fixed_image_id,'BOX(5000 3884,6664 4896)'::box2d),
('west-conus-6664-3884','west-conus'::fixed_image_id,'BOX(6664 3884,8332 4896)'::box2d),
('west-conus-8332-3884','west-conus'::fixed_image_id,'BOX(8332 3884,10000 4896)'::box2d),
('west-conus-0-4896','west-conus'::fixed_image_id,'BOX(0 4896,1668 6000)'::box2d),
('west-conus-1668-4896','west-conus'::fixed_image_id,'BOX(1668 4896,3332 6000)'::box2d),
('west-conus-3332-4896','west-conus'::fixed_image_id,'BOX(3332 4896,5000 6000)'::box2d),
('west-conus-5000-4896','west-conus'::fixed_image_id,'BOX(5000 4896,6664 6000)'::box2d),
('west-conus-6664-4896','west-conus'::fixed_image_id,'BOX(6664 4896,8332 6000)'::box2d),
('west-conus-8332-4896','west-conus'::fixed_image_id,'BOX(8332 4896,10000 6000)'::box2d)
)
    insert into abi_fixed_image_block (fixed_image_block_id,fixed_image_id,box)
select i.fixed_image_block_id,i.fixed_image_id,i.box
  from i left join abi_fixed_image_block b using (fixed_image_block_id)
 where b is null;

create or replace function wsen (
in b abi_fixed_image_block,
out geometry('Polygon'))
LANGUAGE SQL AS $$
with n as (
  select
    (st_xmin(i.angles)+st_xmin(b.box)*angle_inc)*sat_height as west,
    (st_ymax(i.angles)-st_ymax(b.box)*angle_inc)*sat_height as south,
    (st_xmin(i.angles)+st_xmax(b.box)*angle_inc)*sat_height as east,
    (st_ymax(i.angles)-st_ymin(b.box)*angle_inc)*sat_height as north,
    angle_inc*sat_height as res,
    g.srid
    from abi_fixed_image i
         join abi g using (goes_id)
   where b.fixed_image_id=i.fixed_image_id
)
  select
  st_setsrid(
    st_makebox2d(
      st_makepoint(west,south),st_makepoint(east,north)),srid) as boundary
      from n;
  $$;

create table if not exists roi (
roi_id text primary key,
srid integer references spatial_ref_sys,
unaligned_box box2d,
box box2d,
boundary geometry('Polygon')
);

create or replace function alignedTo (
in roi roi, in cnt integer default 10,
out box box2d, out boundary geometry('Polygon'))
LANGUAGE SQL AS $$
with n as (select
(st_xmin(roi.unaligned_box)/500/cnt) as xn,
(st_ymin(roi.unaligned_box)/500/cnt) as yn,
(st_xmax(roi.unaligned_box)/500/cnt) as xx,
(st_ymax(roi.unaligned_box)/500/cnt) as yx
),
nx as (
select
st_makebox2d(
 st_makepoint(case when(xn<0) then floor(xn)*500*cnt else ceil(xn)*500*cnt end,
 case when(yn<0) then floor(yn)*500*cnt else ceil(yn)*500*cnt end),
 st_makepoint(case when(xx<0) then floor(xx)*500*cnt else ceil(xx)*500*cnt end,
  case when(yx<0) then floor(yx)*500*cnt else ceil(yx)*500*cnt end)) as box
from n
)
select box,st_setsrid(box,roi.srid) as boundary
from nx;
$$;

with b(roi_id,srid,unaligned_box) as (
 values ('ca',3310,'BOX(-410000 -660000,610000 460000)'::BOX2D)
)
insert into roi (roi_id,srid,unaligned_box)
select roi_id,srid,unaligned_box
from b;
-- And now update these ROIs to be an aligned region
update roi set boundary=(alignedTo(roi)).boundary, box=(alignedTo(roi)).box;

create or replace function empty_raster (
  in roi roi, in in_size integer default 1,
  out raster)
  LANGUAGE SQL AS $$
  select
        st_setsrid(
          st_makeEmptyRaster(
            ((st_xmax(roi.boundary)-st_xmin(roi.boundary))/(in_size*500))::integer,
            ((st_ymax(roi.boundary)-st_ymin(roi.boundary))/(in_size*500))::integer,
            st_xmin(roi.boundary),
            st_ymax(roi.boundary),in_size*500),roi.srid)
$$;

create materialized view roi_x_fixed_image_block as
  select roi_id,fixed_image_block_id
    from abi g join abi_fixed_image fi using (goes_id)
         join abi_fixed_image_block b using (fixed_image_id)
         join roi r on st_intersects(b.wsen,st_transform(r.boundary,g.srid));

create or replace function fixed_image_block_id (
in block blocks_ring_buffer,
out text)
LANGUAGE SQL AS $$
  select format('%s-%s-%s-%s',block.satellite,block.product,block.x*size,block.y*size)
      from abibands join abiresolution res using (resolution)
     where abibands.band=block.band
  $$;

create or replace function resolution (
  in block blocks_ring_buffer,
  out float)
  LANGUAGE SQL AS $$
  select
  angle_inc*sat_height as resolution
  from abi
  join abi_fixed_image i using (goes_id),
  abibands b join abiresolution using (resolution)
  where block.satellite::goes_id=abi.goes_id
  and block.band=b.band
  and i.image_id=block.product::image_id
  $$;

create or replace function wsen (
in block blocks_ring_buffer,
out geometry('Polygon'))
LANGUAGE SQL AS $$
  with n as (
    select
      (st_xmin(i.angles)+block.x*size*angle_inc)*sat_height as west,
      (st_ymax(i.angles)-(block.y+(st_metadata(block.rast)).height)*size*angle_inc)*sat_height as south,
      (st_xmin(i.angles)+(block.x+(st_metadata(block.rast)).width)*size*angle_inc)*sat_height as east,
      (st_ymax(i.angles)-block.y*size*angle_inc)*sat_height as north,
      angle_inc*sat_height as resolution,
      srid
      from abi join abi_fixed_image i using (goes_id),
           abibands b join abiresolution using (resolution)
     where block.satellite::goes_id=abi.goes_id
       and block.band=b.band
       and i.image_id=block.product::image_id
  )
  select
  st_setsrid(
    st_makebox2d(
      st_makepoint(west,south),st_makepoint(east,north)),srid) as boundary
  from n
$$;

create or replace function image (
in block blocks_ring_buffer,
out raster)
LANGUAGE SQL AS $$
  select st_setsrid(
    st_setGeoReference(block.rast,
                       st_xmin(wsen(block)),
                       st_ymax(wsen(block)),
                       block.resolution,
                       -block.resolution,0,0),
                       st_srid(block.wsen))
  $$;

create table roi_buffer (
  roi_buffer_id serial primary key,
  roi_id text references roi(roi_id),
  date timestamp,
  product text,
  band integer references abibands(band),
  rast raster,
  unique(roi_id,date,product,band)
);

create or replace function blocks_to_roi (
  in in_date timestamp,
  in in_band abibands,
  in roi roi,
  in in_product text,
  out raster)
LANGUAGE SQL AS $$
with
blocks_in as (
  select image(rb) as image
    from
      blocks_ring_buffer rb
      join roi_x_fixed_image_block f on rb.fixed_image_block_id=f.fixed_image_block_id
   where rb.date=in_date
     and rb.band=in_band.band
     and roi.roi_id=f.roi_id
     and in_product=rb.product
)
select
st_clip(
  st_transform(st_union(image),empty_raster(roi,in_band.size)),
  roi.boundary) as rast
  from
  blocks_in
  $$;
