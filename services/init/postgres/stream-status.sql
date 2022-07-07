CREATE TABLE IF NOT EXISTS stream_status (
  stream_status_id SERIAL PRIMARY KEY,
  date timestamp NOT NULL DEFAULT NOW(),
  satellite TEXT NOT NULL,
  apid TEXT NOT NULL,
  channel TEXT NOT NULL,
  UNIQUE(satellite, apid, channel)
);
CREATE INDEX IF NOT EXISTS stream_status_date_idx ON stream_status (date);
CREATE INDEX IF NOT EXISTS stream_status_channel_idx ON stream_status (channel);

CREATE OR REPLACE FUNCTION get_status_latest_timestamp(channel_in text) RETURNS TIMESTAMP AS $$   
DECLARE
  tsp TIMESTAMP;
BEGIN
  
  select date into tsp 
  from stream_status 
  where channel = channel_in
  order by date desc
  limit 1;

  if (tsp is NULL) then
    RAISE EXCEPTION 'Unknown channel: %', channel_in;
  END IF;
  
  RETURN tsp;
END ; 
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION insert_status_latest_timestamp(satellite_in text, channel_in text, apid_in text) RETURNS VOID AS $$   
DECLARE
  ssid INTEGER;
BEGIN
  
  select stream_status_id into ssid 
    from stream_status 
    where channel = channel_in and satellite = satellite_in and apid = apid_in;

  if (ssid is NULL) then
    INSERT INTO stream_status (satellite, channel, apid) VALUES (satellite_in, channel_in, apid_in);
  ELSE
    UPDATE stream_status SET date = now() 
    WHERE channel = channel_in and satellite = satellite_in and apid = apid_in;
  END IF;

END ; 
$$ LANGUAGE plpgsql;

