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

-- access by px
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