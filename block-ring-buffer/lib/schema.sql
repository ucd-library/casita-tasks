ALTER DATABASE casita SET postgis.enable_outdb_rasters TO True;
ALTER DATABASE casita SET postgis.gdal_enabled_drivers TO 'ENABLE_ALL';

CREATE TABLE IF NOT EXISTS blocks_ring_buffer (
  blocks_ring_buffer_id SERIAL PRIMARY KEY,
  date timestamp NOT NULL,
  x INTEGER NOT NULL,
  y INTEGER NOT NULL,
  satellite TEXT NOT NULL,
  product TEXT NOT NULL,
  apid TEXT NOT NULL,
  band INTEGER NOT NULL,
  expire timestamp NOT NULL,
  rast RASTER NOT NULL
);
CREATE INDEX IF NOT EXISTS blocks_ring_buffer_date_idx ON blocks_ring_buffer (date);
CREATE INDEX IF NOT EXISTS blocks_ring_buffer_product_idx ON blocks_ring_buffer (product);
CREATE INDEX IF NOT EXISTS blocks_ring_buffer_rast_idx  ON blocks_ring_buffer USING GIST (ST_ConvexHull(rast));

CREATE TABLE IF NOT EXISTS thermal_product (
  thermal_product_id SERIAL PRIMARY KEY,
  blocks_ring_buffer_id INTEGER REFERENCES blocks_ring_buffer (blocks_ring_buffer_id),
  product TEXT NOT NULL,
  expire timestamp NOT NULL,
  rast RASTER NOT NULL,
  UNIQUE(blocks_ring_buffer_id, product)
);

CREATE TABLE IF NOT EXISTS blocks_ring_buffer_grouped (
  blocks_ring_buffer_grouped_id SERIAL PRIMARY KEY,
  date timestamp NOT NULL,
  x INTEGER NOT NULL,
  y INTEGER NOT NULL,
  satellite TEXT NOT NULL,
  product TEXT NOT NULL,
  apid TEXT NOT NULL,
  band INTEGER NOT NULL,
  type TEXT NOT NULL,
  expire timestamp NOT NULL,
  rast RASTER NOT NULL,
  UNIQUE(x, y, product, date, type)
);
CREATE INDEX IF NOT EXISTS blocks_ring_buffer_grouped_x_idx ON blocks_ring_buffer (x);
CREATE INDEX IF NOT EXISTS blocks_ring_buffer_grouped_y_idx ON blocks_ring_buffer (y);
CREATE INDEX IF NOT EXISTS blocks_ring_buffer_grouped_product_idx ON blocks_ring_buffer (product);

CREATE OR REPLACE FUNCTION b7_variance_detection(x_in INTEGER, y_in INTEGER, product_in TEXT, stdevs INTEGER) 
RETURNS table (
  blocks_ring_buffer_id INTEGER,
  rast RASTER,
  date DATE,
  expire DATE
)
AS $$

  WITH latestId AS (
    SELECT 
      MAX(blocks_ring_buffer_id) AS rid 
    FROM blocks_ring_buffer WHERE 
      band = 7 AND x = x_in AND y = y_in AND product = product_in
  ),
  latest AS (
    SELECT 
      rast, blocks_ring_buffer_id AS rid, date, expire,
      extract(hour from date ) as end, 
      extract(hour from date - interval '2 hour') as start
    FROM latestId, blocks_ring_buffer WHERE 
      blocks_ring_buffer_id = rid
  ),
  rasters AS (
    SELECT 
      rb.rast, blocks_ring_buffer_id AS rid 
    FROM blocks_ring_buffer rb, latest WHERE 
      band = 7 AND x = x_in AND y = y_in AND product = product_in AND 
      blocks_ring_buffer_id != latest.rid AND
      extract(hour from rb.date) >= latest.start AND
      extract(hour from rb.date) <= latest.end 
  ),
  totalCount AS (
    SELECT count(*) AS v FROM rasters
  ),
  total AS (
    SELECT 
      ST_AddBand(ST_MakeEmptyRaster(r.rast), 1, '16BUI'::TEXT, tc.v, -1) AS v 
    FROM rasters r, totalCount tc limit 1
  ),
  avg AS (
    SELECT ST_Union(rast, 'MEAN') AS v FROM rasters	
  ),
  difference AS (
    SELECT 
      ST_MapAlgebra(r.rast, a.v, 'power([rast1.val] - [rast2.val], 2)') AS v 
    FROM rasters r, avg a
  ),
  sum AS (
    SELECT 
      ST_Union(d.v, 'SUM') AS v 
    FROM difference d
  ),
  stdev as (
    SELECT 
      ST_MapAlgebra(s.v, t.v, 'sqrt([rast1.val] / [rast2.val])') AS v 
    FROM sum s, total t	
  ),
  avgDiff AS (
    SELECT 
      ST_MapAlgebra(a.v, l.rast, '[rast2.val] - [rast1.val]') as V 
    FROM avg a, latest l	
  ),
  stdevRatio AS (
    SELECT 
      ST_MapAlgebra(ad.v, sd.v, 'FLOOR([rast1.val] / ([rast2.val]*' || stdevs::TEXT || '))') AS v 
    FROM avgDiff ad, stdev sd	
  )
  SELECT 
    rid AS blocks_ring_buffer_id,
    ST_Reclass(v, 1, '1-65535: 1', '8BUI', 0) AS rast,
    latest.date as date,
    latest.expire as expire
  FROM 
    stdevRatio, latest
$$
LANGUAGE SQL;

--- get the raster to use for stats
CREATE OR REPLACE FUNCTION get_rasters_for_stats (
  blocks_ring_buffer_id_in INTEGER
) RETURNS table (
  blocks_ring_buffer_id INTEGER
) AS $$
  WITH time_range AS (
    SELECT 
      date, product, band, x, y,
      extract(hour from date) as end, 
      extract(hour from date) as start
    FROM blocks_ring_buffer WHERE 
      blocks_ring_buffer_id_in = blocks_ring_buffer_id
  )
  SELECT 
    rb.blocks_ring_buffer_id as blocks_ring_buffer_id
  FROM 
    blocks_ring_buffer rb, time_range 
  WHERE 
    rb.band = time_range.band AND 
    rb.x = time_range.x AND 
    rb.y = time_range.y AND 
    rb.product = time_range.product AND 
    rb.blocks_ring_buffer_id != blocks_ring_buffer_id_in AND
    extract(hour from rb.date) >= time_range.start AND
    extract(hour from rb.date) <= time_range.end AND
    rb.date < time_range.date
$$
LANGUAGE SQL;

CREATE OR REPLACE FUNCTION get_rasters_for_group_stats (
  blocks_ring_buffer_grouped_id_in INTEGER
) RETURNS table (
  blocks_ring_buffer_grouped_id INTEGER
) AS $$
  WITH time_range AS (
    SELECT 
      date, product, band, x, y,
      extract(hour from date) + 4 as end, 
      extract(hour from date) as start
    FROM blocks_ring_buffer_grouped WHERE 
      blocks_ring_buffer_grouped_id_in = blocks_ring_buffer_grouped_id
  )
  SELECT
    rb.blocks_ring_buffer_grouped_id as blocks_ring_buffer_grouped_id
  FROM 
    blocks_ring_buffer_grouped rb, time_range 
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


CREATE OR REPLACE FUNCTION create_thermal_products ( 
  blocks_ring_buffer_id_in INTEGER
) RETURNS void
AS $$
DECLARE
  average_id INTEGER;
  max_id INTEGER;
  min_id INTEGER;
  stddev_id INTEGER;
  rasters INTEGER;
BEGIN

  SELECT thermal_product_id INTO average_id 
    FROM thermal_product
    WHERE blocks_ring_buffer_id = blocks_ring_buffer_id_in
    AND product = 'average';

  SELECT thermal_product_id INTO max_id 
    FROM thermal_product
    WHERE blocks_ring_buffer_id = blocks_ring_buffer_id_in
    AND product = 'max';

  SELECT thermal_product_id INTO min_id 
    FROM thermal_product
    WHERE blocks_ring_buffer_id = blocks_ring_buffer_id_in
    AND product = 'min';

  SELECT thermal_product_id INTO stddev_id 
    FROM thermal_product
    WHERE blocks_ring_buffer_id = blocks_ring_buffer_id_in
    AND product = 'stddev';

  IF( average_id IS NOT NULL ) THEN
    RAISE EXCEPTION 'Average product already exists for blocks_ring_buffer_id %s', blocks_ring_buffer_id_in;
  END IF;

  IF( max_id IS NOT NULL ) THEN
    RAISE EXCEPTION 'Max product already exists for blocks_ring_buffer_id %s', blocks_ring_buffer_id_in;
  END IF;

  IF( min_id IS NOT NULL ) THEN
    RAISE EXCEPTION 'Min product already exists for blocks_ring_buffer_id %s', blocks_ring_buffer_id_in;
  END IF;

  IF( stddev_id IS NOT NULL ) THEN
    RAISE EXCEPTION 'Stddev product already exists for blocks_ring_buffer_id %s', blocks_ring_buffer_id_in;
  END IF; 

  -- AVERAGE
  WITH input as (
    SELECT * from blocks_ring_buffer where blocks_ring_buffer_id = blocks_ring_buffer_id_in
  ),
  rasters as (
    SELECT rast FROM get_rasters_for_stats(blocks_ring_buffer_id_in) as stats
    LEFT JOIN blocks_ring_buffer ON stats.blocks_ring_buffer_id = blocks_ring_buffer.blocks_ring_buffer_id
  )
  INSERT INTO thermal_product (blocks_ring_buffer_id, product, expire, rast)
    SELECT 
      i.blocks_ring_buffer_id as blocks_ring_buffer_id,
      'average' as product,
      i.expire as expire,
      ST_Reclass(
        ST_Union(
          ST_Reclass(r.rast, '0-65536:0-65536', '32BUI')
        , 'MEAN')
      , '0-65536:0-65536', '16BUI') AS rast
    FROM rasters r, input i
    GROUP BY blocks_ring_buffer_id, expire;

  -- MAX
  WITH input as (
    SELECT * from blocks_ring_buffer where blocks_ring_buffer_id = blocks_ring_buffer_id_in
  ),
  rasters as (
    SELECT rast FROM get_rasters_for_stats(blocks_ring_buffer_id_in) as stats
    LEFT JOIN blocks_ring_buffer ON stats.blocks_ring_buffer_id = blocks_ring_buffer.blocks_ring_buffer_id
  )
  INSERT INTO thermal_product (blocks_ring_buffer_id, product, expire, rast)
    SELECT 
      i.blocks_ring_buffer_id as blocks_ring_buffer_id,
      'max' as product,
      i.expire as expire,
      ST_Union(r.rast, 'MAX') AS rast
    FROM rasters r, input i
    GROUP BY blocks_ring_buffer_id, expire;

  -- MIN
  WITH input as (
    SELECT * from blocks_ring_buffer where blocks_ring_buffer_id = blocks_ring_buffer_id_in
  ),
  rasters as (
    SELECT rast FROM get_rasters_for_stats(blocks_ring_buffer_id_in) as stats
    LEFT JOIN blocks_ring_buffer ON stats.blocks_ring_buffer_id = blocks_ring_buffer.blocks_ring_buffer_id
  )
  INSERT INTO thermal_product (blocks_ring_buffer_id, product, expire, rast)
    SELECT 
      i.blocks_ring_buffer_id as blocks_ring_buffer_id,
      'min' as product,
      i.expire as expire,
      ST_Union(r.rast, 'MIN') AS rast
    FROM rasters r, input i
    GROUP BY blocks_ring_buffer_id, expire;

  -- STDDEV
  WITH input as (
    SELECT * from blocks_ring_buffer where blocks_ring_buffer_id = blocks_ring_buffer_id_in
  ),
  rasters as (
    SELECT rast FROM get_rasters_for_stats(blocks_ring_buffer_id_in) as stats
    LEFT JOIN blocks_ring_buffer ON stats.blocks_ring_buffer_id = blocks_ring_buffer.blocks_ring_buffer_id
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
    SELECT * FROM thermal_product WHERE blocks_ring_buffer_id = blocks_ring_buffer_id_in AND product = 'average'
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
  INSERT INTO thermal_product (blocks_ring_buffer_id, product, expire, rast)
    SELECT 
      i.blocks_ring_buffer_id as blocks_ring_buffer_id,
      'stddev' as product,
      i.expire as expire,
      ST_Reclass(
        ST_MapAlgebra(s.rast, t.rast, 'sqrt([rast1.val] / [rast2.val])'),
        '0-65536:0-65536', '16BUI'
      ) AS rast
    FROM sum s, total t, input i;

END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION create_thermal_grouped_products ( 
  blocks_ring_buffer_grouped_id_in INTEGER
) RETURNS void
AS $$
DECLARE
  average_id INTEGER;
  max_id INTEGER;
  min_id INTEGER;
  stddev_id INTEGER;
  rasters INTEGER;
BEGIN

  SELECT blocks_ring_buffer_grouped_id INTO average_id 
    FROM blocks_ring_buffer_grouped
    WHERE blocks_ring_buffer_grouped_id = blocks_ring_buffer_grouped_id_in
    AND type = 'amax-average';

  SELECT blocks_ring_buffer_grouped_id INTO max_id 
    FROM blocks_ring_buffer_grouped
    WHERE blocks_ring_buffer_grouped_id = blocks_ring_buffer_grouped_id_in
    AND type = 'amax-max';

  SELECT blocks_ring_buffer_grouped_id INTO max_id 
    FROM blocks_ring_buffer_grouped
    WHERE blocks_ring_buffer_grouped_id = blocks_ring_buffer_grouped_id_in
    AND type = 'amax-min';

  SELECT blocks_ring_buffer_grouped_id INTO stddev_id 
    FROM blocks_ring_buffer_grouped
    WHERE blocks_ring_buffer_grouped_id = blocks_ring_buffer_grouped_id_in
    AND type = 'amax-stddev';

  IF( average_id IS NOT NULL ) THEN
    RAISE WARNING 'Average product already exists for blocks_ring_buffer_grouped_id %s', blocks_ring_buffer_grouped_id_in;
    RETURN;
  END IF;

  IF( max_id IS NOT NULL ) THEN
    RAISE WARNING 'Max product already exists for blocks_ring_buffer_grouped_id %s', blocks_ring_buffer_grouped_id_in;
    RETURN;
  END IF;

  IF( min_id IS NOT NULL ) THEN
    RAISE WARNING 'Min product already exists for blocks_ring_buffer_grouped_id %s', blocks_ring_buffer_grouped_id_in;
    RETURN;
  END IF;

  IF( stddev_id IS NOT NULL ) THEN
    RAISE WARNING 'Stddev product already exists for blocks_ring_buffer_grouped_id %s', blocks_ring_buffer_grouped_id_in;
    RETURN;
  END IF; 

  -- AVERAGE
  WITH input as (
    SELECT *, date_trunc('hour', date) as hdate from blocks_ring_buffer_grouped 
    where blocks_ring_buffer_grouped_id = blocks_ring_buffer_grouped_id_in AND
    type = 'max'
  ),
  rasters as (
    SELECT rast FROM get_rasters_for_group_stats(blocks_ring_buffer_grouped_id_in) as stats
    LEFT JOIN blocks_ring_buffer_grouped ON blocks_ring_buffer_grouped.blocks_ring_buffer_grouped_id = stats.blocks_ring_buffer_grouped_id
  )
  INSERT INTO blocks_ring_buffer_grouped (date, x, y, satellite, product, apid, band, type, expire, rast)
    SELECT 
      hdate as date,
      i.x as x,
      i.y as y,
      i.satellite as satellite,
      i.product as product,
      i.apid as apid,
      i.band as band,
      'amax-average' as type,
      i.expire as expire,
      ST_Reclass(
        ST_Union(
          ST_Reclass(r.rast, '0-65536:0-65536', '32BUI')
        , 'MEAN')
      , '0-65536:0-65536', '16BUI') AS rast
    FROM rasters r, input i
    GROUP BY hdate, i.x, i.y, i.satellite, i.product, i.apid, i.band, i.expire;

  -- MAX
  WITH input as (
    SELECT *, date_trunc('hour', date) as hdate from blocks_ring_buffer_grouped 
    where blocks_ring_buffer_grouped_id = blocks_ring_buffer_grouped_id_in AND
    type = 'max'
  ),
  rasters as (
    SELECT * FROM get_rasters_for_group_stats(blocks_ring_buffer_grouped_id_in) as stats
    LEFT JOIN blocks_ring_buffer_grouped ON blocks_ring_buffer_grouped.blocks_ring_buffer_grouped_id = stats.blocks_ring_buffer_grouped_id
  )
  INSERT INTO blocks_ring_buffer_grouped (date, x, y, satellite, product, apid, band, type, expire, rast)
    SELECT 
      hdate as date,
      i.x as x,
      i.y as y,
      i.satellite as satellite,
      i.product as product,
      i.apid as apid,
      i.band as band,
      'amax-max' as type,
      i.expire as expire,
      ST_Union(r.rast, 'MAX') AS rast
    FROM rasters r, input i
    GROUP BY hdate, i.x, i.y, i.satellite, i.product, i.apid, i.band, i.expire;

  -- MIN
  WITH input as (
    SELECT *, date_trunc('hour', date) as hdate from blocks_ring_buffer_grouped 
    where blocks_ring_buffer_grouped_id = blocks_ring_buffer_grouped_id_in AND
    type = 'min'
  ),
  rasters as (
    SELECT * FROM get_rasters_for_group_stats(blocks_ring_buffer_grouped_id_in) as stats
    LEFT JOIN blocks_ring_buffer_grouped ON blocks_ring_buffer_grouped.blocks_ring_buffer_grouped_id = stats.blocks_ring_buffer_grouped_id
  )
  INSERT INTO blocks_ring_buffer_grouped (date, x, y, satellite, product, apid, band, type, expire, rast)
    SELECT 
      hdate as date,
      i.x as x,
      i.y as y,
      i.satellite as satellite,
      i.product as product,
      i.apid as apid,
      i.band as band,
      'amax-min' as type,
      i.expire as expire,
      ST_Union(r.rast, 'MIN') AS rast
    FROM rasters r, input i
    GROUP BY hdate, i.x, i.y, i.satellite, i.product, i.apid, i.band, i.expire;

  -- STDDEV
  WITH input as (
    SELECT *, date_trunc('hour', date) as hdate from blocks_ring_buffer_grouped 
    where blocks_ring_buffer_grouped_id = blocks_ring_buffer_grouped_id_in AND
    type = 'max'
  ),
  rasters as (
    SELECT * FROM get_rasters_for_group_stats(blocks_ring_buffer_grouped_id_in) as stats
    LEFT JOIN blocks_ring_buffer_grouped ON blocks_ring_buffer_grouped.blocks_ring_buffer_grouped_id = stats.blocks_ring_buffer_grouped_id
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
    SELECT * FROM blocks_ring_buffer_grouped WHERE blocks_ring_buffer_grouped_id = blocks_ring_buffer_grouped_id_in AND product = 'amax-average'
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
  INSERT INTO blocks_ring_buffer_grouped (date, x, y, satellite, product, apid, band, type, expire, rast)
    SELECT 
      hdate as date,
      i.x as x,
      i.y as y,
      i.satellite as satellite,
      i.product as product,
      i.apid as apid,
      i.band as band,
      'amax-stddev' as type,
      i.expire as expire,
      ST_Reclass(
        ST_MapAlgebra(s.rast, t.rast, 'sqrt([rast1.val] / [rast2.val])'),
        '0-65536:0-65536', '16BUI'
      ) AS rast
    FROM sum s, total t, input i;

END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_thermal_classified_product( blocks_ring_buffer_id_in INTEGER, stddev_ratio INTEGER ) 
RETURNS RASTER
AS $$

  WITH image AS (
    SELECT 
      rast
    FROM blocks_ring_buffer WHERE 
      blocks_ring_buffer_id = blocks_ring_buffer_id_in
  ),
  avg as (
    SELECT 
      rast
    FROM thermal_product WHERE
      blocks_ring_buffer_id = blocks_ring_buffer_id_in AND
      product = 'average'
  ),
  stddev as (
    SELECT 
      rast
    FROM thermal_product WHERE
      blocks_ring_buffer_id = blocks_ring_buffer_id_in AND
      product = 'stddev'
  ),
  avgDiff AS (
    SELECT 
      ST_MapAlgebra(a.rast, i.rast, '[rast2.val] - [rast1.val]') as rast
    FROM avg a, image i	
  ),
  stdevRatio AS (
    SELECT 
      ST_MapAlgebra(ad.rast, sd.rast, 'FLOOR([rast1.val] / ([rast2.val]*' || stddev_ratio::TEXT || '))') AS v 
    FROM avgDiff ad, stddev sd	
  )
  SELECT 
    ST_Reclass(v, 1, '1-65535: 1', '8BUI', 0)
  FROM 
    stdevRatio
$$
LANGUAGE SQL;


CREATE OR REPLACE FUNCTION get_grouped_classified_product( blocks_ring_buffer_id_in INTEGER, stddev_ratio INTEGER ) 
RETURNS RASTER
AS $$

  WITH image AS (
    SELECT 
      rast, x, y, date, product
    FROM blocks_ring_buffer WHERE 
      blocks_ring_buffer_id = blocks_ring_buffer_id_in
  ),
  avg as (
    SELECT 
      br.rast
    FROM blocks_ring_buffer_grouped br, image WHERE
      br.x = image.x AND
      br.y = image.y AND
      br.product = image.product AND
      br.type = 'amax-max'
  ),
  stddev as (
    SELECT 
      br.rast
    FROM blocks_ring_buffer_grouped br, image WHERE
      br.x = image.x AND
      br.y = image.y AND
      br.product = image.product AND
      br.type = 'amax-stddev'
  ),
  avgDiff AS (
    SELECT 
      ST_MapAlgebra(a.rast, i.rast, '[rast2.val] - [rast1.val]') as rast
    FROM avg a, image i	
  ),
  stdevRatio AS (
    SELECT 
      ST_MapAlgebra(ad.rast, sd.rast, 'FLOOR([rast1.val] / ([rast2.val]*' || stddev_ratio::TEXT || '))') AS v 
    FROM avgDiff ad, stddev sd	
  )
  SELECT 
    ST_Reclass(v, 1, '1-65535: 1', '8BUI', 0)
  FROM 
    stdevRatio
$$
LANGUAGE SQL;

CREATE OR REPLACE FUNCTION get_blocks_px_values (
  blocks_ring_buffer_id_in INTEGER,
  x_in INTEGER,
  y_in INTEGER
) RETURNS table (
  value INTEGER,
  blocks_ring_buffer_id INTEGER,
  date TIMESTAMP
) AS $$

  WITH ids AS (
    SELECT get_rasters_for_stats(blocks_ring_buffer_id_in) AS id
  )
  SELECT 
    ST_Value(rast, x_in, y_in) as value,  blocks_ring_buffer_id, date 
  FROM 
    blocks_ring_buffer, ids 
  WHERE blocks_ring_buffer_id = id;

$$
LANGUAGE SQL;

CREATE OR REPLACE FUNCTION get_all_blocks_px_values (
  blocks_ring_buffer_id_in INTEGER,
  x_in INTEGER,
  y_in INTEGER
) RETURNS table (
  value INTEGER,
  blocks_ring_buffer_id INTEGER,
  date TIMESTAMP
) AS $$

  WITH start AS (
    SELECT date, x, y, product
    FROM  blocks_ring_buffer
    WHERE blocks_ring_buffer_id = blocks_ring_buffer_id_in
  ),
  rasters AS (
    SELECT 
      blocks_ring_buffer.blocks_ring_buffer_id as blocks_ring_buffer_id,
      blocks_ring_buffer.rast as rast,
      blocks_ring_buffer.date as date
    from blocks_ring_buffer, start 
    where blocks_ring_buffer.date <= start.date
    AND blocks_ring_buffer.x = start.x
    AND blocks_ring_buffer.y = start.y
    AND blocks_ring_buffer.product = start.product
  )
  SELECT 
    ST_Value(rast, x_in, y_in) as value, blocks_ring_buffer_id, date 
  FROM 
    rasters;

$$
LANGUAGE SQL;

CREATE OR REPLACE FUNCTION create_hourly_max (
  product_in TEXT,
  x_in INTEGER,
  y_in INTEGER,
  date_in TIMESTAMP
) RETURNS INTEGER AS $$
  DECLARE
    brbgid INTEGER;
  BEGIN

  SELECT
    blocks_ring_buffer_grouped_id into brbgid
  FROM 
    blocks_ring_buffer_grouped
  WHERE
    x = x_in AND y = y_in AND
    product = product_in AND 
    date = date_trunc('hour', date_in) AND
    type = 'max';
  
  IF( brbgid IS NOT NULL ) THEN
    RAISE WARNING 'Max product already exists for blocks_ring_buffer % % % %', product_in, x_in, y_in, date_in;
    RETURN -1;
  END IF;

  WITH rasters AS (
    SELECT 
      rast,
      x_in AS x,
      y_in AS y,
      product_in AS product,
      apid, band, satellite
    FROM  blocks_ring_buffer
    WHERE 
      product = product_in AND
      x = x_in AND 
      y = y_in AND
      date_trunc('hour', date) = date_trunc('hour', date_in)
  )
  INSERT INTO blocks_ring_buffer_grouped (date, x, y, satellite, product, apid, band, rast, type, expire)
  SELECT 
    date_trunc('hour', date_in) as date,
    x, y, satellite, product, apid, band,
    ST_Union(rast, 'max') as rast, 
    'max' as type,
    (date_in + interval '10 days') as expire
  FROM rasters
  GROUP BY x, y, product, apid, band, satellite;

  SELECT
    blocks_ring_buffer_grouped_id into brbgid
  FROM 
    blocks_ring_buffer_grouped
  WHERE
    x = x_in AND y = y_in AND
    product = product_in AND 
    date = date_trunc('hour', date_in) AND
    type = 'max';

  RAISE INFO 'Create Max product for blocks_ring_buffer % % % %', product_in, x_in, y_in, date_in;

  RETURN brbgid;
END;
$$ LANGUAGE plpgsql;