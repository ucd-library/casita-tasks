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

CREATE TABLE IF NOT EXISTS goesr_raster_block (
  goesr_raster_block_id SERIAL PRIMARY KEY,
  x INTEGER NOT NULL,
  y INTEGER NOT NULL,
  satellite TEXT NOT NULL,
  product TEXT NOT NULL,
  apid TEXT NOT NULL,
  band INTEGER NOT NULL,
  UNIQUE(x, y, satellite, product, apid, band)
);
CREATE INDEX IF NOT EXISTS goesr_raster_block_product_idx ON goesr_raster_block (product);
CREATE INDEX IF NOT EXISTS goesr_raster_block_x_idx ON goesr_raster_block (x);
CREATE INDEX IF NOT EXISTS goesr_raster_block_y_idx ON goesr_raster_block (y);

CREATE OR REPLACE FUNCTION get_block_product_id (
  satellite_in TEXT, product_in TEXT, band_in INTEGER, x_in INTEGER, y_in INTEGER
) RETURNS INTEGER AS $$ 
DECLARE
  grbid INTEGER;
BEGIN

  SELECT goesr_raster_block_id INTO grbid
  WHERE satellite = satellite_in AND product = product_in AND
  band = band_in and x = x_in and y = y_in;

  IF ( grbid IS NULL ) THEN
    INSERT INTO goesr_raster_block_id( satellite, product, band, x, y)
    VALUES (satellite_in, product_in, band_in, x_in, y_in);

    SELECT goesr_raster_block_id INTO grbid
    WHERE satellite = satellite_in AND product = product_in AND
    band = band_in and x = x_in and y = y_in;
  END IF;

  RETURN grbid;
END;
$$ LANGUAGE plpgsql;

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