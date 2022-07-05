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