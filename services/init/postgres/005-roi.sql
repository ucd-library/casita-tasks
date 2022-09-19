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
insert into
  abibands (band,wavelength,region,name,resolution)
select
  b.band,b.wavelength,b.region,b.name,b.resolution::abiResolution_id
from
  bands b
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
select
  goes_id, i.sat_longitude, i.sat_height, i.angle_ul,i.angle_inc, i.srid, i.proj4text
from i
left join abi using (goes_id) where abi is null;

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

with ul(fixed_image_id,row,w,h,cols) as (
     values
('west-fulldisk',0,1808,604,'{7232,9040,10848,12656}'),
('west-fulldisk',604,1808,1012,'{3616,5424,7232,9040,10848,12656,14464,16272}'),
('west-fulldisk',1616,1808,1012,'{3616,5424,7232,9040,10848,12656,14464,16272}'),
('west-fulldisk',2628,1808,1012,'{1808,3616,5424,7232,9040,10848,12656,14464,16272,18080}'),
('west-fulldisk',3640,1808,1012,'{1808,3616,5424,7232,9040,10848,12656,14464,16272,18080}'),
('west-fulldisk',4652,1808,1012,'{0,1808,3616,5424,7232,9040,10848,12656,14464,16272,18080,19888}'),
('west-fulldisk',5664,1808,1012,'{0,1808,3616,5424,7232,9040,10848,12656,14464,16272,18080,19888}'),
('west-fulldisk',6676,1808,1012,'{0,1808,3616,5424,7232,9040,10848,12656,14464,16272,18080,19888}'),
('west-fulldisk',7688,1808,1012,'{0,1808,3616,5424,7232,9040,10848,12656,14464,16272,18080,19888}'),
('west-fulldisk',8700,1808,1012,'{0,1808,3616,5424,7232,9040,10848,12656,14464,16272,18080,19888}'),
('west-fulldisk',9712,1808,1012,'{0,1808,3616,5424,7232,9040,10848,12656,14464,16272,18080,19888}'),
('west-fulldisk',10724,1808,1008,'{0,1808,3616,5424,7232,9040,10848,12656,14464,16272,18080,19888}'),
('west-fulldisk',11732,1808,1012,'{0,1808,3616,5424,7232,9040,10848,12656,14464,16272,18080,19888}'),
('west-fulldisk',12744,1808,1012,'{0,1808,3616,5424,7232,9040,10848,12656,14464,16272,18080,19888}'),
('west-fulldisk',13756,1808,1012,'{0,1808,3616,5424,7232,9040,10848,12656,14464,16272,18080,19888}'),
('west-fulldisk',14768,1808,1012,'{0,1808,3616,5424,7232,9040,10848,12656,14464,16272,18080,19888}'),
('west-fulldisk',15780,1808,1012,'{0,1808,3616,5424,7232,9040,10848,12656,14464,16272,18080,19888}'),
('west-fulldisk',16792,1808,1012,'{0,1808,3616,5424,7232,9040,10848,12656,14464,16272,18080,19888}'),
('west-fulldisk',17804,1808,1012,'{1808,3616,5424,7232,9040,10848,12656,14464,16272,18080}'),
('west-fulldisk',18816,1808,1012,'{1808,3616,5424,7232,9040,10848,12656,14464,16272,18080}'),
('west-fulldisk',19828,1808,1012,'{3616,5424,7232,9040,10848,12656,14464,16272}'),
('west-fulldisk',20840,1808,856,'{5424,7232,9040,10848,12656,14464}'),
('west-conus',0,1668,852,'{0,3332,6664,8332}'),
('west-conus',0,1664,852,'{1668,5000}'),
('west-conus',852,1668,1008,'{0,3332,6664,8332}'),
('west-conus',852,1664,1008,'{1668,5000}'),
('west-conus',1860,1668,1012,'{0,3332,6664,8332}'),
('west-conus',1860,1664,1012,'{1668,5000}'),
('west-conus',2872,1668,1012,'{0,3332,6664,8332}'),
('west-conus',2872,1664,1012,'{1668,5000}'),
('west-conus',3884,1668,1012,'{0,3332,6664,8332}'),
('west-conus',3884,1664,1012,'{1668,5000}'),
('west-conus',4896,1668,1104,'{0,3332,6664,8332}'),
('west-conus',4896,1664,1104,'{1668,5000}')),
u as (
  select fixed_image_id,w,h,row,unnest(cols::integer[]) as col
    from ul
)
       insert into abi_fixed_image_block
   select format('%s-%s-%s',fixed_image_id,col,row) as fixed_image_block_id,
          fixed_image_id::fixed_image_id,
          st_makebox2d(st_makepoint(col,row),st_makepoint(col+w,row+h)) as box
     from u

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
    g.resolution as res,
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
res_500m float,
unaligned_box box2d,
box box2d,
boundary geometry('Polygon')
);

create or replace function alignedTo (
in roi roi,
in sz int,
out box box2d, out boundary geometry('Polygon'))
LANGUAGE SQL AS $$
with n as (select
(st_xmin(roi.unaligned_box)/(roi.res_500m*sz)) as xn,
(st_ymin(roi.unaligned_box)/(roi.res_500m*sz)) as yn,
(st_xmax(roi.unaligned_box)/(roi.res_500m*sz)) as xx,
(st_ymax(roi.unaligned_box)/(roi.res_500m*sz)) as yx
),
nx as (
select
st_makebox2d(
 st_makepoint(case when(xn<0) then floor(xn)*(roi.res_500m*sz) else ceil(xn)*(roi.res_500m*sz) end,
 case when(yn<0) then floor(yn)*(roi.res_500m*sz) else ceil(yn)*(roi.res_500m*sz) end),
 st_makepoint(case when(xx<0) then floor(xx)*(roi.res_500m*sz) else ceil(xx)*(roi.res_500m*sz) end,
  case when(yx<0) then floor(yx)*(roi.res_500m*sz) else ceil(yx)*(roi.res_500m*sz) end)) as box
from n
)
select box,st_setsrid(box,roi.srid) as boundary
from nx;
$$;

with b(roi_id,srid,res_500m,unaligned_box) as (
 values ('ca',3310,500,'BOX(-410000 -660000,610000 460000)'::BOX2D)
)
insert into roi (roi_id,srid,unaligned_box)
select roi_id,srid,unaligned_box
from b;
-- And now update these ROIs to be an aligned region
update roi set boundary=(alignedTo(roi,10)).boundary, box=(alignedTo(roi,10)).box;

with w as (
    select *,abi.resolution from abi where goes_id='west'
  ),
    roi_w as (
      select goes_id,format('%s-goes-%s',roi_id,goes_id) as roi_id,
             st_transform(boundary,w.srid) as bound
        from roi,w
    )
      insert into roi(roi_id,srid,res_500m,unaligned_box)
  select
    roi_id,w.srid,w.resolution,bound
    from w join roi_w using (goes_id);
  -- update ROIs to aligned region
update roi
   set boundary=(alignedTo(roi,10)).boundary,
       box=(alignedTo(roi,10)).box
       from abi where
                   roi.srid=abi.srid;

create or replace function empty_raster (
  in roi roi, in in_size integer default 1,
  out raster)
  LANGUAGE SQL AS $$
  select
        st_setsrid(
          st_makeEmptyRaster(
            ((st_xmax(roi.boundary)-st_xmin(roi.boundary))/(in_size*roi.res_500m))::integer,
            ((st_ymax(roi.boundary)-st_ymin(roi.boundary))/(in_size*roi.res_500m))::integer,
            st_xmin(roi.boundary),
            st_ymax(roi.boundary),in_size*roi.res_500m),roi.srid)
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
  angle_inc*sat_height*size as resolution
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

create table products (
  product_id text primary key,
  description text
  );

insert into products(product_id,description)
values
('conus','Per band CONUS image product'),
('fulldisk','Per band fulldisk image product'),
('ca-hourly-max','Per band hourly max value'),
('ca-hourly-max-10d-average','10 day running average of ca-hourly-max'),
('ca-hourly-max-10d-max','10 day running max of ca-hourly-max'),
('ca-hourly-max-10d-min','10 day running min of ca-hourly-max'),
('ca-hourly-max-10d-stddev','10 day running stddev of ca-hourly-max')
;

create table roi_buffer (
  roi_buffer_id serial primary key,
  roi_id text references roi(roi_id),
  product_id text references products(product_id),
  band integer references abibands(band),
  date timestamp,
  expire timestamp,
  rast raster,
  boundary geometry('Polygon'),
  unique(roi_id,date,product_id,band)
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

create or replace function blocks_to_roi (
  in in_date timestamp,
  in in_band_number integer,
  in in_roi_id text,
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
     and rb.band=in_band_number
     and in_roi_id=f.roi_id
     and in_product=rb.product
)
select
st_clip(
  st_transform(st_union(image),
               empty_raster((select roi from roi where roi_id=in_roi_id),
               (select b.size from abiBands b where b.band=in_band_number))),
  (select boundary from roi where roi_id=in_roi_id)) as rast
  from
  blocks_in
  $$;
