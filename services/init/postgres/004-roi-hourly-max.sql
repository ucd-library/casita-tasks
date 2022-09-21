CREATE OR REPLACE FUNCTION create_hourly_max (
  roi_in TEXT,
  band_in INTEGER,
  date_in TIMESTAMP
) RETURNS INTEGER AS $$
  DECLARE
    brbid INTEGER;
    brhbid INTEGER;
    testcount INTEGER;
    hmax_product_name TEXT;
  BEGIN

  SELECT roi_in || '-hourly-max' into  hmax_product_name;

  SELECT
    roi_buffer_id into brbid
  FROM 
    roi_buffer
  WHERE
    band = band_in AND
    product_id = hmax_product_name AND 
    date = date_trunc('hour', date_in);
  
  SELECT 
    count(*) into testcount
  FROM roi_buffer
  WHERE 
    roi_id = roi_in AND
    band = band_in AND
    date_trunc('hour', date) = date_trunc('hour', date_in);
  
  RAISE INFO 'count %', testcount;


  IF( brbid IS NOT NULL ) THEN
    RAISE WARNING 'Max product already exists for roi_buffer % % %', hmax_product_name,  band_in, date_trunc('hour', date_in);
    RETURN brbid;
  END IF;

  WITH rasters AS (
    SELECT 
      rast,
      product_id AS product,
      roi_id as roi, 
      band
    FROM roi_buffer
    WHERE 
      roi_id = roi_in AND
      band = band_in AND
      date_trunc('hour', date) = date_trunc('hour', date_in)
  )
  INSERT INTO roi_buffer (date, product_id, roi_id, band, rast, expire)
  SELECT 
    date_trunc('hour', date_in) as date,
    hmax_product_name,
    roi, 
    band,
    ST_Union(rast, 'max') as rast,
    (date_in + interval '10 days') as expire
  FROM rasters
  GROUP BY roi, band;

  SELECT
    roi_buffer_id into brhbid
  FROM 
    roi_buffer
  WHERE
    product_id = hmax_product_name AND 
    date = date_trunc('hour', date_in);

  INSERT INTO derived_stats_metadata (roi_buffer_id, parent_id, date, product, roi, band)
  SELECT
    brhbid as roi_buffer_id,
    roi_buffer_id as parent_block_id,
    date, product_id, roi_id, band
  FROM
    roi_buffer
  WHERE
    roi_id = roi_in AND
    band = band_in AND
    date_trunc('hour', date) = date_trunc('hour', date_in);

  RAISE INFO 'Created Max product for roi_buffer % % %', brhbid, roi_in, date_trunc('hour', date_in);

  RETURN brhbid;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION create_all_hourly_max (
  roi_in TEXT,
  band_in INTEGER
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
  FROM roi_buffer
  WHERE 
    roi_id = roi_in AND
    band = band_in;

  SELECT 
    EXTRACT(EPOCH FROM max(date) - min(date))/3600 INTO hours    
  FROM roi_buffer
  WHERE 
    roi_id = roi_in AND
    band = band_in;

  index := 0;
  WHILE index < hours LOOP
    select create_hourly_max(roi_in, band_in, min_date + interval '1 hour' * index ) into hid;
    index := index + 1;
  END LOOP;

  RAISE INFO 'Created Max product for roi_buffer %, % hours: %', roi_in, band_in, hours;

  SELECT roi_in || '-hourly-max' into  hmax_product_name;

  SELECT
    roi_buffer_id into brbid
  FROM 
    roi_buffer
  WHERE
    band = band_in AND
    product_id = hmax_product_name
  ORDER BY date desc
  LIMIT 1;

  RETURN brbid;

END;
$$ LANGUAGE plpgsql;