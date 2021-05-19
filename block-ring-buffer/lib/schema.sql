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