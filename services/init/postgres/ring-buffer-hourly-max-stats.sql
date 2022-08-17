-- create the [product]-hourly-max-10d-[stat] rasters where stat is
-- average, max, min and stddev.  The stats are generated using a 4
-- hour window (2 hours prior to current hour and 2 hours past) for
-- the last 10 days (see get_rasters_for_hmax_stats)
CREATE OR REPLACE FUNCTION create_hourly_max_stats ( 
  blocks_ring_buffer_id_in INTEGER
) RETURNS TIMESTAMP
AS $$
DECLARE
  lastest_hmax_id INTEGER;
  average_id INTEGER;
  max_id INTEGER;
  min_id INTEGER;
  stddev_id INTEGER;
  rasters INTEGER;
  x_in INTEGER;
  y_in INTEGER;
  band_in INTEGER;
  product_in TEXT;
  date_in TIMESTAMP;
BEGIN

  SELECT 
    br.x, br.y, br.product, br.band INTO x_in, y_in, product_in, band_in
    FROM blocks_ring_buffer br
    WHERE br.blocks_ring_buffer_id = blocks_ring_buffer_id_in;

  SELECT create_all_hourly_max(product_in, band_in, x_in, y_in) INTO lastest_hmax_id;  

  SELECT 
    br.date, br.product, br.blocks_ring_buffer_id INTO date_in, product_in, blocks_ring_buffer_id_in
    FROM blocks_ring_buffer br
    WHERE br.blocks_ring_buffer_id = lastest_hmax_id;
  
  RAISE INFO 'Creating stats for % % % %', product_in, date_in, x_in, y_in;

  SELECT blocks_ring_buffer_id INTO average_id 
    FROM blocks_ring_buffer
    WHERE 
    x = x_in AND y = y_in 
    AND date = date_in
    AND product = product_in || '-10d-average';

  SELECT blocks_ring_buffer_id INTO max_id 
    FROM blocks_ring_buffer
    WHERE
    x = x_in AND y = y_in
    AND date = date_in
    AND product = product_in || '-10d-max';

  SELECT blocks_ring_buffer_id INTO min_id 
    FROM blocks_ring_buffer
    WHERE
    x = x_in AND y = y_in
    AND date = date_in
    AND product = product_in || '-10d-min';

  SELECT blocks_ring_buffer_id INTO stddev_id 
    FROM blocks_ring_buffer
    WHERE
    x = x_in AND y = y_in 
    AND date = date_in
    AND product = product_in || '-10d-stddev';

  -- AVERAGE
  IF( average_id IS NOT NULL ) THEN
    RAISE WARNING 'Average product already exists for blocks_ring_buffer_id %, id=%', blocks_ring_buffer_id_in, average_id;
  ELSE

    WITH input as (
      SELECT *, date_trunc('hour', date) as hdate from blocks_ring_buffer 
      where blocks_ring_buffer_id = blocks_ring_buffer_id_in
    ),
    rasters as (
      SELECT rast FROM get_rasters_for_hmax_stats(blocks_ring_buffer_id_in)
    )
    INSERT INTO blocks_ring_buffer (date, x, y, satellite, product, apid, band, expire, rast)
      SELECT 
        hdate as date,
        i.x as x,
        i.y as y,
        i.satellite as satellite,
        product_in || '-10d-average' as product,
        i.apid as apid,
        i.band as band,
        i.expire as expire,
        ST_Reclass(
          ST_Union(
            ST_Reclass(r.rast, '0-65536:0-65536', '32BUI')
          , 'MEAN')
        , '0-65536:0-65536', '16BUI') AS rast
      FROM rasters r, input i
      GROUP BY hdate, i.x, i.y, i.satellite, i.product, i.apid, i.band, i.expire
    RETURNING blocks_ring_buffer_id INTO average_id;

    INSERT INTO derived_blocks_metadata (blocks_ring_buffer_id, parent_block_id, date, x, y, satellite, product, apid, band)
    SELECT
      average_id as blocks_ring_buffer_id,
      blocks_ring_buffer_id as parent_block_id,
      date, x, y, satellite, product, apid, band
    FROM get_rasters_for_hmax_stats(blocks_ring_buffer_id_in);

  END IF;


  -- MAX
  IF( max_id IS NOT NULL ) THEN
    RAISE WARNING 'Max product already exists for blocks_ring_buffer_id %, id=%', blocks_ring_buffer_id_in, max_id;
  ELSE
  
    WITH input as (
      SELECT *, date_trunc('hour', date) as hdate from blocks_ring_buffer 
      where blocks_ring_buffer_id = blocks_ring_buffer_id_in
    ),
    rasters as (
      SELECT * FROM get_rasters_for_hmax_stats(blocks_ring_buffer_id_in)
    )
    INSERT INTO blocks_ring_buffer (date, x, y, satellite, product, apid, band, expire, rast)
      SELECT 
        hdate as date,
        i.x as x,
        i.y as y,
        i.satellite as satellite,
        product_in || '-10d-max',
        i.apid as apid,
        i.band as band,
        i.expire as expire,
        ST_Union(r.rast, 'MAX') AS rast
      FROM rasters r, input i
      GROUP BY hdate, i.x, i.y, i.satellite, i.product, i.apid, i.band, i.expire
    RETURNING blocks_ring_buffer_id INTO max_id;

    INSERT INTO derived_blocks_metadata (blocks_ring_buffer_id, parent_block_id, date, x, y, satellite, product, apid, band)
    SELECT
      max_id as blocks_ring_buffer_id,
      blocks_ring_buffer_id as parent_block_id,
      date, x, y, satellite, product, apid, band
    FROM get_rasters_for_hmax_stats(blocks_ring_buffer_id_in);
  END IF;

  -- MIN
  IF( min_id IS NOT NULL ) THEN
    RAISE WARNING 'Min product already exists for blocks_ring_buffer_id %, id=%', blocks_ring_buffer_id_in, min_id;

  ELSE

    WITH input as (
      SELECT *, date_trunc('hour', date) as hdate from blocks_ring_buffer 
      where blocks_ring_buffer_id = blocks_ring_buffer_id_in
    ),
    rasters as (
      SELECT * FROM get_rasters_for_hmax_stats(blocks_ring_buffer_id_in)
    )
    INSERT INTO blocks_ring_buffer (date, x, y, satellite, product, apid, band, expire, rast)
      SELECT 
        hdate as date,
        i.x as x,
        i.y as y,
        i.satellite as satellite,
        product_in || '-10d-min',
        i.apid as apid,
        i.band as band,
        i.expire as expire,
        ST_Union(r.rast, 'MIN') AS rast
      FROM rasters r, input i
      GROUP BY hdate, i.x, i.y, i.satellite, i.product, i.apid, i.band, i.expire
    RETURNING blocks_ring_buffer_id INTO min_id;

    INSERT INTO derived_blocks_metadata (blocks_ring_buffer_id, parent_block_id, date, x, y, satellite, product, apid, band)
    SELECT
      min_id as blocks_ring_buffer_id,
      blocks_ring_buffer_id as parent_block_id,
      date, x, y, satellite, product, apid, band
    FROM get_rasters_for_hmax_stats(blocks_ring_buffer_id_in);
  END IF;

  -- STDDEV
  IF( stddev_id IS NOT NULL ) THEN
    RAISE WARNING 'Stddev product already exists for blocks_ring_buffer_id %, id=%', blocks_ring_buffer_id_in, stddev_id;

  ELSE

    WITH input as (
      SELECT *, date_trunc('hour', date) as hdate from blocks_ring_buffer 
      where blocks_ring_buffer_id = blocks_ring_buffer_id_in
    ),
    rasters as (
      SELECT * FROM get_rasters_for_hmax_stats(blocks_ring_buffer_id_in)
    ),
    totalCount AS (
      SELECT count(*) AS v FROM rasters
    ),
    total AS (
      SELECT 
        ST_AddBand(ST_MakeEmptyRaster(r.rast), 1, '32BUI'::TEXT, tc.v, -1) AS rast 
      FROM rasters r, totalCount tc limit 1
    ),
    avg as (
      SELECT bg.* FROM blocks_ring_buffer bg, input
      WHERE bg.date = input.hdate AND bg.x = input.x AND bg.y = input.y 
      AND bg.product = input.product || '-10d-average'
    ),
    difference AS (
      SELECT 
        ST_MapAlgebra(
          ST_Reclass(r.rast, '0-65536:0-65536', '32BUI'), 
          ST_Reclass(a.rast, '0-65536:0-65536', '32BUI'), 
          'power([rast1.val] - [rast2.val], 2)'
        ) AS rast 
      FROM rasters r, avg a
    ),
    sum AS (
      SELECT 
        ST_Union(d.rast, 'SUM') AS rast 
      FROM difference d
    )
    INSERT INTO blocks_ring_buffer (date, x, y, satellite, product, apid, band, expire, rast)
      SELECT 
        hdate as date,
        i.x as x,
        i.y as y,
        i.satellite as satellite,
        product_in || '-10d-stddev',
        i.apid as apid,
        i.band as band,
        i.expire as expire,
        ST_Reclass(
          ST_MapAlgebra(s.rast, t.rast, 'sqrt([rast1.val] / [rast2.val])'),
          '0-65536:0-65536', '16BUI'
        ) AS rast
      FROM sum s, total t, input i
    RETURNING blocks_ring_buffer_id INTO stddev_id;

    INSERT INTO derived_blocks_metadata (blocks_ring_buffer_id, parent_block_id, date, x, y, satellite, product, apid, band)
    SELECT
      stddev_id as blocks_ring_buffer_id,
      blocks_ring_buffer_id as parent_block_id,
      date, x, y, satellite, product, apid, band
    FROM get_rasters_for_hmax_stats(blocks_ring_buffer_id_in);

    INSERT INTO derived_blocks_metadata (blocks_ring_buffer_id, parent_block_id, date, x, y, satellite, product, apid, band)
    SELECT
      stddev_id as blocks_ring_buffer_id,
      average_id as parent_block_id,
      date, x, y, satellite, product, apid, band
    FROM 
      blocks_ring_buffer
    WHERE
      blocks_ring_buffer_id = average_id;

  END IF; 

  RETURN date_in;

END;
$$ LANGUAGE plpgsql;

-- Grab a 4 hour window of the last 10 days of hourly max rasters
CREATE OR REPLACE FUNCTION get_rasters_for_hmax_stats (
  blocks_ring_buffer_id_in INTEGER
) RETURNS table (
  blocks_ring_buffer_id INTEGER,
  rast RASTER,
  date TIMESTAMP,
  x INTEGER,
  y INTEGER,
  band INTEGER,
  product TEXT,
  satellite TEXT,
  apid TEXT
) AS $$
  WITH time_range AS (
    SELECT 
      date, product, band, x, y,
      extract(hour from date) + 2 as end, 
      extract(hour from date) - 2 as start
    FROM blocks_ring_buffer WHERE 
      blocks_ring_buffer_id_in = blocks_ring_buffer_id
  )
  SELECT
    rb.blocks_ring_buffer_id as blocks_ring_buffer_id,
    rb.rast as rast,
    rb.date as date,
    rb.x as x,
    rb.y as y,
    rb.band as band,
    rb.product as product,
    rb.satellite as satellite,
    rb.apid as apid
  FROM 
    blocks_ring_buffer rb, time_range 
  WHERE 
    rb.band = time_range.band AND 
    rb.x = time_range.x AND 
    rb.y = time_range.y AND 
    rb.product = time_range.product AND 
    date_trunc('day', rb.date) != date_trunc('day', time_range.date) AND
    extract(hour from rb.date) >= time_range.start AND
    extract(hour from rb.date) <= time_range.end AND
    rb.date < time_range.date
$$
LANGUAGE SQL;