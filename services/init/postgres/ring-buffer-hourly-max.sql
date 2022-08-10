CREATE OR REPLACE FUNCTION create_hourly_max (
  product_in TEXT,
  x_in INTEGER,
  y_in INTEGER,
  date_in TIMESTAMP
) RETURNS INTEGER AS $$
  DECLARE
    brbid INTEGER;
    hmax_product_name TEXT;
  BEGIN

  SELECT product_in || '-hourly-max' into  hmax_product_name;

  SELECT
    blocks_ring_buffer_id into brbid
  FROM 
    blocks_ring_buffer
  WHERE
    x = x_in AND y = y_in AND
    product = hmax_product_name AND 
    date = date_trunc('hour', date_in);
  
  IF( brbid IS NOT NULL ) THEN
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
    FROM blocks_ring_buffer
    WHERE 
      product = product_in AND
      x = x_in AND 
      y = y_in AND
      date_trunc('hour', date) = date_trunc('hour', date_in)
  )
  INSERT INTO blocks_ring_buffer (date, x, y, satellite, product, apid, band, rast, expire)
  SELECT 
    date_trunc('hour', date_in) as date,
    x, y, satellite, hmax_product_name, apid, band,
    ST_Union(rast, 'max') as rast,
    (date_in + interval '10 days') as expire
  FROM rasters
  GROUP BY x, y, product, apid, band, satellite;

  SELECT
    blocks_ring_buffer_id into brbid
  FROM 
    blocks_ring_buffer
  WHERE
    x = x_in AND y = y_in AND
    product = hmax_product_name AND 
    date = date_trunc('hour', date_in);

  RAISE INFO 'Create Max product for blocks_ring_buffer % % % %', product_in, x_in, y_in, date_in;

  RETURN brbid;
END;
$$ LANGUAGE plpgsql;