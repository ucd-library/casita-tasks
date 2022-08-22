CREATE TABLE IF NOT EXISTS web_push_subscriptions (
  web_push_subscriptions_id SERIAL PRIMARY KEY,
  created timestamp NOT NULL DEFAULT NOW(),
  payload JSONB NOT NULL,
  UNIQUE(payload)
);

CREATE TABLE IF NOT EXISTS web_push_notifications (
  web_push_notifications_id SERIAL PRIMARY KEY,
  web_push_subscriptions_id INTEGER REFERENCES web_push_subscriptions(web_push_subscriptions_id),
  type TEXT NOT NULL,
  UNIQUE(web_push_subscriptions_id, type)
);

CREATE OR REPLACE VIEW web_push_notifications_view AS
  SELECT 
    n.web_push_notifications_id,
    s.web_push_subscriptions_id,
    s.payload,
    n.type
  FROM 
    web_push_notifications n
  LEFT JOIN web_push_subscriptions s ON s.web_push_subscriptions_id = n.web_push_subscriptions_id;

CREATE OR REPLACE FUNCTION add_web_push_notification(type_in text, payload_in JSONB) RETURNS VOID AS $$   
DECLARE
  wpnid INTEGER;
BEGIN
  
  SELECT 
    web_push_notifications_id into wpnid
  FROM 
    web_push_notifications
  WHERE payload->>"endpoint" = payload_in->>"endpoint";

  IF ( wpnid is NULL) THEN
    INSERT INTO 
      web_push_notifications (payload) 
    VALUES
      (payload_in)
    RETURNING web_push_notifications_id INTO wpnid;
  END IF;

  INSERT INTO 
    web_push_notifications_types (web_push_notifications_id, type)
  VALUES
    (wpnid, type_in);

END ; 
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION remove_web_push_notifications(endpoint_in text) RETURNS VOID AS $$   
DECLARE
  wpnid INTEGER;
BEGIN
  
  WITH sub AS ( 
    select web_push_notifications_id from web_push_notifications where payload->>"endpoint" = endpoint_in 
  ) 
  DELETE FROM 
    web_push_notifications CASCADE 
  WHERE
    sub.web_push_notifications_id = web_push_notifications.web_push_notifications_id;

  DELETE FROM 
    web_push_subscriptions 
  WHERE 
    payload->>"endpoint" = endpoint_in;

END ; 
$$ LANGUAGE plpgsql;