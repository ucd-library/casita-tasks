CREATE TABLE IF NOT EXISTS thermal_event (
  thermal_event_id SERIAL PRIMARY KEY,
  created timestamp DEFAULT CURRENT_TIMESTAMP,
  label TEXT,
  active BOOLEAN DEFAULT TRUE
);
CREATE INDEX IF NOT EXISTS thermal_event_id_idx ON thermal_event (thermal_event_id);
CREATE INDEX IF NOT EXISTS thermal_event_active_idx ON thermal_event (active);

CREATE TABLE IF NOT EXISTS thermal_event_px (
  thermal_event_px_id SERIAL PRIMARY KEY,
  thermal_event_id INTEGER REFERENCES thermal_event NOT NULL,
  goesr_raster_block_id INTEGER REFERENCES goesr_raster_block,
  date timestamp NOT NULL,
  world_x INTEGER NOT NULL,
  world_y INTEGER NOT NULL,
  pixel_x INTEGER NOT NULL,
  pixel_y INTEGER NOT NULL,
  value INTEGER NOT NULL,
  UNIQUE(thermal_event_id, date, world_x, world_y)
);
CREATE INDEX IF NOT EXISTS thermal_event_px_id_idx ON thermal_event_px (thermal_event_id);
CREATE INDEX IF NOT EXISTS thermal_event_px_x_idx ON thermal_event_px (world_x);
CREATE INDEX IF NOT EXISTS thermal_event_px_y_idx ON thermal_event_px (world_y);

CREATE OR REPLACE VIEW active_thermal_events AS
  SELECT
    te.thermal_event_id,
    te.created,
    b.satellite,
    b.product,
    b.apid,
    b.band,
    te.active,
    tep.world_x,
    tep.world_y,
    tep.value,
    tep.date
  FROM thermal_event te 
  LEFT JOIN thermal_event_px tep ON te.thermal_event_id = tep.thermal_event_id
  LEFT JOIN goesr_raster_block b ON tep.goesr_raster_block_id = b.goesr_raster_block_id
  WHERE te.active = true;

CREATE TABLE IF NOT EXISTS thermal_event_px_product (
  thermal_event_px_product_id SERIAL PRIMARY KEY,
  goesr_raster_block_id INTEGER REFERENCES goesr_raster_block,
  date timestamp NOT NULL,
  type TEXT NOT NULL,
  pixel_x INTEGER NOT NULL,
  pixel_y INTEGER NOT NULL,
  value INTEGER NOT NULL,
  UNIQUE(date, satellite, band, product, block_x, block_y, type, pixel_x, pixel_y)
);
CREATE INDEX IF NOT EXISTS thermal_event_px_product_id_idx ON thermal_event_px_product (thermal_event_px_product_id);
CREATE INDEX IF NOT EXISTS thermal_event_px_product_type_idx ON thermal_event_px_product (date);

CREATE TABLE IF NOT EXISTS thermal_event_px_history (
  thermal_event_px_history_id SERIAL PRIMARY KEY,
  thermal_event_px_id INTEGER REFERENCES thermal_event_px NOT NULL,
  thermal_event_px_product_id INTEGER REFERENCES thermal_event_px_product NOT NULL,
  UNIQUE(thermal_event_px_id, thermal_event_px_product_id)
);
-- CREATE INDEX IF NOT EXISTS thermal_event_px_history_px_id_idx ON thermal_event_px_history (thermal_event_px_id);
-- CREATE INDEX IF NOT EXISTS thermal_event_px_history_product_id_idx ON thermal_event_px_history (thermal_event_px_product_id);

CREATE OR REPLACE VIEW thermal_event_history AS
  SELECT h.thermal_event_px_id, p.thermal_event_px_product_id, p.type, p.date, p.pixel_x, p.pixel_y, p.value,
  b.satellite, b.apid, b.product, b.band, b.x as block_x, b.y as block_y FROM thermal_event_px_history h
  LEFT JOIN thermal_event_px_product p ON p.thermal_event_px_product_id = h.thermal_event_px_product_id
  LEFT JOIN goesr_raster_block b ON p.goesr_raster_block_id = b.goesr_raster_block_id;