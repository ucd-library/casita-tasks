CREATE OR REPLACE FUNCTION create_hourly_max (
  product_in TEXT,
  band_in INTEGER,
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
    band = band_in AND
    product = hmax_product_name AND 
    date = date_trunc('hour', date_in);
  
  IF( brbid IS NOT NULL ) THEN
    RAISE WARNING 'Max product already exists for blocks_ring_buffer % % % % %', product_in,  band_in, x_in, y_in, date_trunc('hour', date_in);
    RETURN brbid;
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
      band = band_in AND
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

  RAISE INFO 'Create Max product for blocks_ring_buffer % % % %', product_in, x_in, y_in, date_trunc('hour', date_in);

  RETURN brbid;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION create_all_hourly_max (
  product_in TEXT,
  band_in INTEGER,
  x_in INTEGER,
  y_in INTEGER
) RETURNS INTEGER AS $$
  DECLARE
    index INTEGER;
    hours INTEGER;
    hid INTEGER;
    min_date TIMESTAMP;
    hmax_product_name TEXT;
    brbid INTEGER;
  BEGIN

  SELECT 
    min(date) into min_date
  FROM blocks_ring_buffer
  WHERE 
    product = product_in AND
    band = band_in AND
    x = x_in and y = y_in;

  SELECT 
    EXTRACT(EPOCH FROM max(date) - min(date))/3600 INTO hours    
  FROM blocks_ring_buffer
  WHERE 
    product = product_in AND
    band = band_in AND
    x = x_in and y = y_in;

  index := 0;
  WHILE index < hours LOOP
    select create_hourly_max(product_in, band_in, x_in, y_in, min_date + interval '1 hour' * index ) into hid;
    index := index + 1;
  END LOOP;

  RAISE INFO 'Create Max product for blocks_ring_buffer %,% hours: %', x_in, y_in, hours;

  SELECT product_in || '-hourly-max' into  hmax_product_name;

  SELECT
    blocks_ring_buffer_id into brbid
  FROM 
    blocks_ring_buffer
  WHERE
    x = x_in AND y = y_in AND
    band = band_in AND
    product = hmax_product_name
  ORDER BY date desc
  LIMIT 1;

  RETURN brbid;

END;
$$ LANGUAGE plpgsql;