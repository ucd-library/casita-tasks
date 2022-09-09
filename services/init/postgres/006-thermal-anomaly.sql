CREATE TABLE IF NOT EXISTS thermal_anomaly_event (
  thermal_anomaly_event_id SERIAL PRIMARY KEY,
  created timestamp DEFAULT CURRENT_TIMESTAMP,
  label TEXT,
  notes TEXT,
  satellite TEXT NOT NULL,
  product TEXT NOT NULL,
  apid TEXT NOT NULL,
  band TEXT NOT NULL,
  active BOOLEAN DEFAULT TRUE
);
CREATE INDEX IF NOT EXISTS thermal_anomaly_event_id_idx ON thermal_anomaly_event (thermal_anomaly_event_id);
CREATE INDEX IF NOT EXISTS thermal_anomaly_event_active_idx ON thermal_anomaly_event (active);

CREATE TABLE IF NOT EXISTS thermal_anomaly_event_px (
  thermal_anomaly_event_px_id SERIAL PRIMARY KEY,
  thermal_anomaly_event_id INTEGER REFERENCES thermal_anomaly_event NOT NULL,
  date timestamp NOT NULL,
  block_x INTEGER NOT NULL,
  block_y INTEGER NOT NULL,
  pixel_x INTEGER NOT NULL,
  pixel_y INTEGER NOT NULL,
  classifier INTEGER NOT NULL,
  value INTEGER NOT NULL,
  UNIQUE(thermal_anomaly_event_id, date, block_x, block_y, pixel_x, pixel_y, classifier)
);
CREATE INDEX IF NOT EXISTS thermal_anomaly_event_px_id_idx ON thermal_anomaly_event_px (thermal_anomaly_event_id);
CREATE INDEX IF NOT EXISTS thermal_anomaly_event_px_date_idx ON thermal_anomaly_event_px (date);
CREATE INDEX IF NOT EXISTS thermal_anomaly_event_px_block_x_idx ON thermal_anomaly_event_px (block_x);
CREATE INDEX IF NOT EXISTS thermal_anomaly_event_px_block_y_idx ON thermal_anomaly_event_px (block_y);

CREATE TABLE IF NOT EXISTS thermal_anomaly_stats_product (
  thermal_anomaly_stats_product_id SERIAL PRIMARY KEY,
  satellite TEXT NOT NULL,
  product TEXT NOT NULL,
  apid TEXT NOT NULL,
  band TEXT NOT NULL,
  UNIQUE(satellite, product, apid, band)
);
CREATE INDEX IF NOT EXISTS thermal_anomaly_stats_product_product_idx ON thermal_anomaly_stats_product (product);

CREATE TABLE IF NOT EXISTS thermal_anomaly_stats_px (
  thermal_anomaly_stats_px_id SERIAL PRIMARY KEY,
  thermal_anomaly_stats_product_id INTEGER REFERENCES thermal_anomaly_stats_product NOT NULL,
  date timestamp NOT NULL,
  block_x INTEGER NOT NULL,
  block_y INTEGER NOT NULL,
  pixel_x INTEGER NOT NULL,
  pixel_y INTEGER NOT NULL,
  value INTEGER NOT NULL,
  UNIQUE(thermal_anomaly_stats_product_id, date, block_x, block_y, pixel_x, pixel_y)
);
CREATE INDEX IF NOT EXISTS thermal_anomaly_stats_px_date_idx ON thermal_anomaly_stats_px (date);
CREATE INDEX IF NOT EXISTS thermal_anomaly_stats_px_product_idx ON thermal_anomaly_stats_px (thermal_anomaly_stats_product_id);

CREATE TABLE IF NOT EXISTS thermal_anomaly_event_px_stats_px (
  thermal_anomaly_event_px_stats_px_id SERIAL PRIMARY KEY,
  thermal_anomaly_event_px_id INTEGER REFERENCES thermal_anomaly_event_px NOT NULL,
  thermal_anomaly_stats_px_id INTEGER REFERENCES thermal_anomaly_stats_px NOT NULL,
  UNIQUE(thermal_anomaly_event_px_id, thermal_anomaly_stats_px_id)
);

CREATE OR REPLACE VIEW thermal_anomaly_stats_px_view AS 
  SELECT
    px.*,
    po.satellite,
    po.product,
    po.band,
    po.apid,
    r.thermal_anomaly_event_px_id
  FROM 
    thermal_anomaly_stats_px px
  LEFT JOIN thermal_anomaly_event_px_stats_px r on r.thermal_anomaly_stats_px_id = px.thermal_anomaly_stats_px_id
  LEFT JOIN thermal_anomaly_stats_product po on po.thermal_anomaly_stats_product_id = px.thermal_anomaly_stats_product_id;

CREATE OR REPLACE VIEW active_thermal_anomaly_events AS
  SELECT
    te.thermal_anomaly_event_id,
    te.created,
    te.satellite,
    te.product,
    te.apid,
    te.band,
    te.active,
    tep.block_x,
    tep.block_y,
    tep.pixel_x,
    tep.pixel_y,
    tep.value,
    tep.date
  FROM thermal_anomaly_event te 
  LEFT JOIN thermal_anomaly_event_px tep ON te.thermal_anomaly_event_id = tep.thermal_anomaly_event_id
  WHERE te.active = true;

CREATE OR REPLACE FUNCTION classify_thermal_anomaly( blocks_ring_buffer_id_in INTEGER, stddev_ratio INTEGER ) 
RETURNS RASTER
AS $$

  WITH image AS (
    SELECT 
      rast, x, y, date, product,
      date_trunc('hour', date - interval '1 hour') as prior_hour
    FROM blocks_ring_buffer WHERE 
      blocks_ring_buffer_id = blocks_ring_buffer_id_in
  ),
  avg as (
    SELECT 
      br.rast
    FROM blocks_ring_buffer br, image WHERE
      br.x = image.x AND
      br.y = image.y AND
      br.product = image.product || '-hourly-max-10d-average' AND 
      br.date = prior_hour
  ),
  stddev as (
    SELECT 
      br.rast
    FROM blocks_ring_buffer br, image WHERE
      br.x = image.x AND
      br.y = image.y AND
      br.product = image.product || '-hourly-max-10d-stddev' AND 
      br.date = prior_hour
  ),
  avgDiff AS (
    SELECT 
      ST_MapAlgebra(a.rast, i.rast, '[rast2.val] - [rast1.val]') as rast
    FROM avg a, image i	
  ),
  stdevRatio AS (
    SELECT 
      ST_MapAlgebra(ad.rast, sd.rast, 'FLOOR([rast1.val] / GREATEST(( GREATEST( 100, LEAST([rast2.val], 500)) *' || stddev_ratio::TEXT || '), 1))') AS v 
    FROM avgDiff ad, stddev sd	
  )
  SELECT 
    ST_Reclass(v, 1, '1-65535: 1', '8BUI', 0)
  FROM 
    stdevRatio
$$
LANGUAGE SQL;

CREATE OR REPLACE FUNCTION all_thermal_anomaly_px_values (
  product_in TEXT,
  block_x_in INTEGER,
  block_y_in INTEGER,
  px_x_in INTEGER,
  px_y_in INTEGER
) RETURNS table (
  value INTEGER,
  blocks_ring_buffer_id INTEGER,
  satellite TEXT,
  product TEXT,
  band TEXT,
  apid TEXT,
  date TIMESTAMP
) AS $$

  SELECT 
    ST_Value(rast, px_x_in, px_y_in) as value,
    blocks_ring_buffer_id,
    satellite,
    product,
    band,
    apid,
    date
  FROM 
    blocks_ring_buffer 
  WHERE 
    x = block_x_in AND 
    y = block_y_in AND 
    ( 
      product = product_in || '-hourly-max-10d-stddev' OR
      product = product_in || '-hourly-max-10d-average' OR
      product = product_in || '-hourly-max-10d-min' OR
      product = product_in || '-hourly-max-10d-max' OR
      product = product_in || '-hourly-max'
    )
  ORDER BY date;

$$
LANGUAGE SQL;