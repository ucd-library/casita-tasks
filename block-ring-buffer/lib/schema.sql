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

CREATE OR REPLACE FUNCTION b7_variance_detection(x_in INTEGER, y_in INTEGER, product_in TEXT, stdevs INTEGER) RETURNS raster
AS $$

  WITH latestId AS (
    SELECT 
      MAX(blocks_ring_buffer_id) AS rid 
    FROM blocks_ring_buffer WHERE 
      band = 7 AND x = x_in AND y = y_in AND product = product_in
  ),
  latest AS (
    SELECT 
      rast, blocks_ring_buffer_id AS rid, date,
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
    ST_Reclass(v, 1, '1-65535: 1', '8BUI', 0) AS v 
  FROM stdevRatio
$$
LANGUAGE SQL;
