-- create the [product]-hourly-max-10d-[stat] rasters where stat is
-- average, max, min and stddev.  The stats are generated using a 4
-- hour window (2 hours prior to current hour and 2 hours past) for
-- the last 10 days (see get_rasters_for_hmax_stats)
CREATE OR REPLACE FUNCTION create_hourly_max_stats ( 
  roi_buffer_id_in INTEGER
) RETURNS TIMESTAMP
AS $$
DECLARE
  lastest_hmax_id INTEGER;
  average_id INTEGER;
  max_id INTEGER;
  min_id INTEGER;
  stddev_id INTEGER;
  rasters INTEGER;
  band_in INTEGER;
  roi_in TEXT;
  date_in TIMESTAMP;
BEGIN

  SELECT 
    br.band, roi_id INTO band_in, roi_in
    FROM roi_buffer br
    WHERE br.roi_buffer_id = roi_buffer_id_in;

  SELECT create_all_hourly_max(roi_in, band_in) INTO lastest_hmax_id;  

  SELECT 
    br.date, br.roi_buffer_id INTO date_in, roi_buffer_id_in
    FROM roi_buffer br
    WHERE br.roi_buffer_id = lastest_hmax_id;
  
  RAISE INFO 'Creating stats for % %', roi_in, date_in;

  SELECT roi_buffer_id INTO average_id 
    FROM roi_buffer
    WHERE 
    date = date_in
    AND product = roi_in || '-10d-average';

  SELECT roi_buffer_id INTO max_id 
    FROM roi_buffer
    WHERE
    date = date_in
    AND product = roi_in || '-10d-max';

  SELECT roi_buffer_id INTO min_id 
    FROM roi_buffer
    WHERE
    date = date_in
    AND product = roi_in || '-10d-min';

  SELECT roi_buffer_id INTO stddev_id 
    FROM roi_buffer
    WHERE
    date = date_in
    AND product = roi_in || '-10d-stddev';

  -- AVERAGE
  IF( average_id IS NOT NULL ) THEN
    RAISE WARNING 'Average product already exists for roi_buffer_id %, id=%', roi_buffer_id_in, average_id;
  ELSE

    WITH input as (
      SELECT *, date_trunc('hour', date) as hdate from roi_buffer 
      where roi_buffer_id = roi_buffer_id_in
    ),
    rasters as (
      SELECT rast FROM get_rasters_for_hmax_stats(roi_buffer_id_in)
    )
    INSERT INTO roi_buffer (date, satellite, product_id, roi_id, band, expire, rast)
      SELECT 
        hdate as date,
        roi_in || '-10d-average' as product_id,
        i.roi_id as roi_id,
        i.band as band,
        i.expire as expire,
        ST_Reclass(
          ST_Union(
            ST_Reclass(r.rast, '0-65536:0-65536', '32BUI')
          , 'MEAN')
        , '0-65536:0-65536', '16BUI') AS rast
      FROM rasters r, input i
      GROUP BY hdate, i.product_id, i.roi_id, i.band, i.expire
    RETURNING roi_buffer_id INTO average_id;

    INSERT INTO derived_blocks_metadata (roi_buffer_id, parent_block_id, date, product, roi, band)
    SELECT
      average_id as roi_buffer_id,
      roi_buffer_id as parent_id,
      date, product, roi, band
    FROM get_rasters_for_hmax_stats(roi_buffer_id_in);

  END IF;


  -- MAX
  IF( max_id IS NOT NULL ) THEN
    RAISE WARNING 'Max product already exists for roi_buffer_id %, id=%', roi_buffer_id_in, max_id;
  ELSE
  
    WITH input as (
      SELECT *, date_trunc('hour', date) as hdate from roi_buffer 
      where roi_buffer_id = roi_buffer_id_in
    ),
    rasters as (
      SELECT * FROM get_rasters_for_hmax_stats(roi_buffer_id_in)
    )
    INSERT INTO roi_buffer (date, product_id, roi_id, band, expire, rast)
      SELECT 
        hdate as date,
        roi_in || '-10d-max',
        i.roi_id as roi_id,
        i.band as band,
        i.expire as expire,
        ST_Union(r.rast, 'MAX') AS rast
      FROM rasters r, input i
      GROUP BY hdate, i.product_id, i.roi_id, i.band, i.expire
    RETURNING roi_buffer_id INTO max_id;

    INSERT INTO derived_blocks_metadata (roi_buffer_id, parent_id, date, product_id, roi_id, band)
    SELECT
      max_id as roi_buffer_id,
      roi_buffer_id as parent_id,
      date, product_id, roi_id, band
    FROM get_rasters_for_hmax_stats(roi_buffer_id_in);
  END IF;

  -- MIN
  IF( min_id IS NOT NULL ) THEN
    RAISE WARNING 'Min product already exists for roi_buffer_id %, id=%', roi_buffer_id_in, min_id;

  ELSE

    WITH input as (
      SELECT *, date_trunc('hour', date) as hdate from roi_buffer 
      where roi_buffer_id = roi_buffer_id_in
    ),
    rasters as (
      SELECT * FROM get_rasters_for_hmax_stats(roi_buffer_id_in)
    )
    INSERT INTO roi_buffer (date, product_id, roi_id, band, expire, rast)
      SELECT 
        hdate as date,
        roi_in || '-10d-min',
        i.roi_id as roi_id,
        i.band as band,
        i.expire as expire,
        ST_Union(r.rast, 'MIN') AS rast
      FROM rasters r, input i
      GROUP BY hdate, i.product_id, i.roi_id, i.band, i.expire
    RETURNING roi_buffer_id INTO min_id;

    INSERT INTO derived_blocks_metadata (roi_buffer_id, parent_id, date, product_id, roi_id, band)
    SELECT
      min_id as roi_buffer_id,
      roi_buffer_id as parent_id,
      date, product_id, roi_id, band
    FROM get_rasters_for_hmax_stats(roi_buffer_id_in);
  END IF;

  -- STDDEV
  IF( stddev_id IS NOT NULL ) THEN
    RAISE WARNING 'Stddev product already exists for roi_buffer_id %, id=%', roi_buffer_id_in, stddev_id;

  ELSE

    WITH input as (
      SELECT *, date_trunc('hour', date) as hdate from roi_buffer 
      where roi_buffer_id = roi_buffer_id_in
    ),
    rasters as (
      SELECT * FROM get_rasters_for_hmax_stats(roi_buffer_id_in)
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
      SELECT bg.* FROM roi_buffer bg, input
      WHERE bg.date = input.hdate
      AND bg.product = input.roi_id || '-10d-average'
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
    INSERT INTO roi_buffer (date, product, roi_id, band, expire, rast)
      SELECT 
        hdate as date,
        roi_in || '-10d-stddev',
        i.roi_id as roi_id,
        i.band as band,
        i.expire as expire,
        ST_Reclass(
          ST_MapAlgebra(s.rast, t.rast, 'sqrt([rast1.val] / [rast2.val])'),
          '0-65536:0-65536', '16BUI'
        ) AS rast
      FROM sum s, total t, input i
    RETURNING roi_buffer_id INTO stddev_id;

    INSERT INTO derived_blocks_metadata (roi_buffer_id, parent_id, date, product, roi, band)
    SELECT
      stddev_id as roi_buffer_id,
      roi_buffer_id as parent_block_id,
      date, product, roi, band
    FROM get_rasters_for_hmax_stats(roi_buffer_id_in);

  END IF; 

  RETURN date_in;

END;
$$ LANGUAGE plpgsql;

-- Grab a 4 hour window of the last 10 days of hourly max rasters
CREATE OR REPLACE FUNCTION get_rasters_for_hmax_stats (
  roi_buffer_id_in INTEGER
) RETURNS table (
  roi_buffer_id INTEGER,
  rast RASTER,
  date TIMESTAMP,
  band INTEGER,
  product TEXT,
  roi TEXT
) AS $$
  WITH time_range AS (
    SELECT 
      date, product_id, roi_id, band,
      extract(hour from date) + 2 as end, 
      extract(hour from date) - 2 as start
    FROM roi_buffer WHERE 
      roi_buffer_id_in = roi_buffer_id
  )
  SELECT
    rb.roi_buffer_id as roi_buffer_id,
    rb.rast as rast,
    rb.date as date,
    rb.band as band,
    rb.product_id as product,
    rb.roi_id as roi
  FROM 
    roi_buffer rb, time_range 
  WHERE 
    rb.band = time_range.band AND 
    rb.roi_id = time_range.roi_id AND 
    rb.product_id = time_range.product_id AND 
    date_trunc('day', rb.date) != date_trunc('day', time_range.date) AND
    extract(hour from rb.date) >= time_range.start AND
    extract(hour from rb.date) <= time_range.end AND
    rb.date < time_range.date
$$
LANGUAGE SQL;