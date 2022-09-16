CREATE TABLE IF NOT EXISTS thermal_anomaly_event (
  thermal_anomaly_event_id SERIAL PRIMARY KEY,
  created timestamp DEFAULT CURRENT_TIMESTAMP,
  label TEXT,
  notes TEXT,
  product TEXT NOT NULL,
  roi TEXT NOT NULL,
  band TEXT NOT NULL,
  active BOOLEAN DEFAULT TRUE
);
CREATE INDEX IF NOT EXISTS thermal_anomaly_event_id_idx ON thermal_anomaly_event (thermal_anomaly_event_id);
CREATE INDEX IF NOT EXISTS thermal_anomaly_event_active_idx ON thermal_anomaly_event (active);

CREATE TABLE IF NOT EXISTS thermal_anomaly_event_start_px (
  thermal_anomaly_event_start_px_id SERIAL PRIMARY KEY,
  thermal_anomaly_event_id INTEGER REFERENCES thermal_anomaly_event NOT NULL,
  date timestamp NOT NULL,
  x INTEGER NOT NULL,
  y INTEGER NOT NULL,
  prior_10d_hmax_average INTEGER[] NOT NULL,
  prior_10d_hmax_stddev INTEGER[] NOT NULL,
  prior_10d_hmax INTEGER[] NOT NULL,
  value INTEGER NOT NULL,
  UNIQUE(thermal_anomaly_event_id, date, x, y)
);
CREATE INDEX IF NOT EXISTS thermal_anomaly_event_start_px_date_idx ON thermal_anomaly_event_start_px (date);
CREATE INDEX IF NOT EXISTS thermal_anomaly_event_start_px_x_idx ON thermal_anomaly_event_start_px (x);
CREATE INDEX IF NOT EXISTS thermal_anomaly_event_start_px_y_idx ON thermal_anomaly_event_start_px (y);

CREATE TABLE IF NOT EXISTS thermal_anomaly_event_px (
  thermal_anomaly_event_px_id SERIAL PRIMARY KEY,
  thermal_anomaly_event_start_px_id INTEGER REFERENCES thermal_anomaly_event_start_px NOT NULL,
  date timestamp NOT NULL,
  classifier INTEGER NOT NULL,
  value INTEGER NOT NULL,
  UNIQUE(thermal_anomaly_event_start_px, date, classifier)
);
CREATE INDEX IF NOT EXISTS thermal_anomaly_event_start_px_id_idx ON thermal_anomaly_event_start_px_id (thermal_anomaly_event_start_px_id);
CREATE INDEX IF NOT EXISTS thermal_anomaly_event_px_date_idx ON thermal_anomaly_event_px (date);
CREATE INDEX IF NOT EXISTS thermal_anomaly_event_px_x_idx ON thermal_anomaly_event_px (x);
CREATE INDEX IF NOT EXISTS thermal_anomaly_event_px_y_idx ON thermal_anomaly_event_px (y);

CREATE OR REPLACE VIEW active_thermal_anomaly_events AS
  SELECT
    te.thermal_anomaly_event_id,
    te.created as event_start,
    te.satellite,
    te.product,
    te.roi,
    te.apid,
    te.band,
    te.active,
    tep.x,
    tep.y,
    tep.date as pixel_start
  FROM thermal_anomaly_event te 
  LEFT JOIN thermal_anomaly_event_start_px tep ON te.thermal_anomaly_event_id = tep.thermal_anomaly_event_id
  WHERE te.active = true;

CREATE OR REPLACE FUNCTION classify_thermal_anomaly( roi_buffer_id_in INTEGER, stddev_ratio INTEGER ) 
RETURNS RASTER
AS $$

  WITH image AS (
    SELECT 
      rast, date, product_id, roi_id
      date_trunc('hour', date - interval '1 hour') as prior_hour
    FROM roi_buffer WHERE 
      roi_buffer_id = roi_buffer_id_in
  ),
  avg as (
    SELECT 
      br.rast
    FROM roi_buffer br, image WHERE
      br.product = image.roi_id || '-hourly-max-10d-average' AND 
      br.date = prior_hour
  ),
  stddev as (
    SELECT 
      br.rast
    FROM roi_buffer br, image WHERE
      br.x = image.x AND
      br.y = image.y AND
      br.product = image.roi_id || '-hourly-max-10d-stddev' AND 
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
  roi_in TEXT,
  px_x_in INTEGER,
  px_y_in INTEGER
) RETURNS table (
  value INTEGER,
  roi_buffer_id INTEGER,
  product TEXT,
  roi TEXT,
  band TEXT,
  date TIMESTAMP
) AS $$

  SELECT 
    ST_Value(rast, px_x_in, px_y_in) as value,
    roi_buffer_id,
    product_id as product,
    roi_id as roi,
    band,
    date
  FROM 
    roi_buffer 
  WHERE
    ( 
      product = roi_in || '-hourly-max-10d-stddev' OR
      product = roi_in || '-hourly-max-10d-average' OR
      product = roi_in || '-hourly-max-10d-min' OR
      product = roi_in || '-hourly-max-10d-max' OR
      product = roi_in || '-hourly-max'
    )
  ORDER BY date;

$$
LANGUAGE SQL;

CREATE OR REPLACE FUNCTION prior_px_values (
  product_in TEXT,
  roi_in TEXT,
  px_x_in INTEGER,
  px_y_in INTEGER
) RETURNS table (
  value INTEGER,
  roi_buffer_id INTEGER,
  product TEXT,
  roi TEXT,
  band TEXT,
  date TIMESTAMP
) AS $$

  SELECT 
    ST_Value(rast, px_x_in, px_y_in) as value,
    roi_buffer_id,
    product_id as product,
    roi_id as roi,
    band,
    date
  FROM 
    roi_buffer 
  WHERE
    product = roi_in AND
    date >= now() - interval '6 hours'
  ORDER BY date;

$$
LANGUAGE SQL;