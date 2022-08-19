import {logger, pg, config} from '@ucd-lib/casita-worker';

class EventDetection {

  constructor() {
    this.eventRadius = config.thermalAnomaly.eventRadius;
  }

  async addClassifiedPixels(blocksRingBufferId, classifier) {
    if( !classifier ) classifier = config.thermalAnomaly.stddevClassifier;

    // grab all classified pixels
    let resp = await pg.query(
      `select * from ST_PixelOfValue(classify_thermal_anomaly($1, $2), 1);`,
      [blocksRingBufferId, classifier]
    );

    let eventSet = {
      new : new Set(),
      continued : new Set()
    }

    // nothing to see here
    if( resp.rows.length === 0 ) {
      return eventSet;
    }

    // grab block product info
    let presp = await pg.query(`SELECT 
        product, x, y, date, satellite, band, apid 
      FROM 
        blocks_ring_buffer 
      WHERE 
        blocks_ring_buffer_id = $1`, 
        [blocksRingBufferId]
    );

    let info = Object.assign({}, presp.rows[0]);
    info.block = {
      x : info.x,
      y : info.y
    };
    info.classifier = classifier;
    delete info.x;
    delete info.y;

    // for each classified pixel, find active event or create new active event
    // then add pixel to event addToActiveThermalEvent
    for( let pixel of resp.rows ) {
      info.pixel = {
        x : pixel.x, 
        y : pixel.y
      }

      try {
        let events = await this.findActiveEvents(info);

        // the historical (stats) pixels will be added to tables in 
        if( events.length ) {
          eventSet.continued.add(await this.addToActiveThermalEvent(events, id, info));
        } else {
          eventSet.new.add(await this.addCreateThermalEvent(id, info));
        }
      } catch(e) {
        console.error(e);
      }
    }


    return eventSet;
  }

  async findActiveEvents(info) {
    let resp = await pg.query(
      `select * from active_thermal_anomaly_events ate WHERE 
        product = $1 AND
        x >= $2 - $4 AND x <= $2 + $4 AND
        y >= $3 - $4 AND y <= $3 + $4`
        [info.product, info.x, info.y, this.eventRadius]
    );
    return resp.rows;
  }

  async addCreateThermalEvent(id, info) {
    logger.info('Creating thermal event for', id, info);

    let resp = await pg.query(`
    INSERT INTO 
      thermal_anomaly_event (created, satellite, product, apid, band)
    VALUES ($1, $2, $3, $4, $5) 
    RETURNING thermal_anomaly_event_id`, [
      info.date.toISOString(), info.satellite, info.product, info.apid, info.band
    ]);

    await this.addToActiveThermalEvent(resp.rows, id, info);

    return resp.rows[0];
  }

  async addToActiveThermalEvent(events, id, info) {
    // currently just use first event
    let event = Array.isArray(events) ? events[0] : events;
    let value = await this.getPxValue(id, info.pixel.x, info.pixel.y);

    logger.info('Adding thermal pixel for', event, info);

    let resp = await pg.query(`
    INSERT INTO thermal_anomaly_event_px (
      thermal_anomaly_event_id, date, block_x, block_y, pixel_x, pixel_y, classifier, value
    ) VALUES (
      $1, $2, $3, $4, $5, $6
    ) RETURNING thermal_anomaly_event_px_id`, 
    [event.thermal_anomaly_event_id, info.date.toISOString(), info.block.x, info.block.y, info.pixel.x, info.pixel.y, classifier, value]);

    // now check add pixels used;
    await this.addPxHistory(resp.rows[0].thermal_anomaly_event_px_id, info);

    return event;
  }

  async addPxHistory(thermal_anomaly_event_px_id, info) {
    // get all stats product pixels for event
    let resp = await pg.query(
      `SELECT * FROM all_thermal_anomaly_px_values($1, $2, $3, $4, $5)`, 
      [info.product, info.block.x, info.block.y, info.pixel.x, info.pixel.y]
    );

    let productIdCache = {};

    for( let row of resp.rows ) {
      let productId = [row.satellite, row.band, row.product, row.apid].join('-');

      // get the product id for the pixel
      if( !productIdCache[productId] ) {
        productIdCache[productId] = await this.getOrCreatePxProduct(info);
      }
      row.thermal_anomaly_stats_product_id = productIdCache[productId];

      row.thermal_anomaly_stats_px_id = await this.getOrCreateStatsPx(info);


      try {
        await pg.query(`INSERT INTO 
            thermal_anomaly_event_px_stats_px (thermal_anomaly_event_px_id, thermal_anomaly_stats_px_id) 
          VALUES
            ($1, $2)`, 
          [thermal_anomaly_event_px_id, row.thermal_anomaly_stats_px_id]
        )
      } catch(e) {
        console.error(e);
      }
    }
  }

  async getOrCreateStatsPx(info) {
    // lookup the product id, create if needed
    let existsResp = await pg.query(`SELECT 
        thermal_anomaly_stats_px_id
      FROM 
        thermal_anomaly_stats_px 
      WHERE
        thermal_anomaly_stats_product_id = $1 AND
        block_x = $2 AND 
        block_y = $3 AND
        pixel_x = $4 AND 
        pixel_y = $5
      `,
      [info.thermal_anomaly_stats_product_id, info.block.x, info.block.y, info.pixel.x, info.pixel.y]
    );

    if( !existsResp.rows.length ) {
      try {
        existsResp = await pg.query(`
          INSERT INTO 
            thermal_anomaly_stats_px (thermal_anomaly_stats_product_id, block_x, block_y, pixel_x, pixel_y) 
          VALUES
            ($1, $2, $3, $4, $5) RETURNING thermal_anomaly_stats_px_id`,
          [info.thermal_anomaly_stats_product_id, info.block.x, info.block.y, info.pixel.x, info.pixel.y]
        );
      } catch(e) {
        console.error(e);
      }
    }

    return existsResp.rows[0].thermal_anomaly_event_px_id;
  }

  async getOrCreatePxProduct(info) {
    // lookup the product id, create if needed
    let existsResp = await pg.query(`SELECT 
        thermal_anomaly_stats_product_id
      FROM 
        thermal_anomaly_stats_product 
      WHERE
        satellite = $1 AND 
        band = $2 AND 
        product = $3 AND 
        apid = $4
      `,
      [info.satellite, info.band, info.product, info.apid]
    );

    if( !existsResp.rows.length ) {
      try {
        existsResp = await pg.query(`
          INSERT INTO 
            thermal_anomaly_stats_product (satellite, product, apid, band) 
          VALUES
            ($1, $2, $3, $4) RETURNING thermal_anomaly_event_px_product_id`,
          [info.satellite, info.band, info.product, info.apid]
        );
      } catch(e) {
        console.error(e);
      }
    }

    return existsResp.rows[0].thermal_anomaly_stats_product_id;
  }

  async getPxValue(blocks_ring_buffer_id, x, y) {
    let resp = await pg.query(`
      SELECT ST_Value(rast, ${x}, ${y}) as value 
      FROM blocks_ring_buffer
      WHERE blocks_ring_buffer_id = $1`, [blocks_ring_buffer_id]);
    if( !resp.rows.length ) return -1;
    return resp.rows[0].value;
  }

}

const instance = new EventDetection();
function run() {
  return instance.addClassifiedPixels(config.blocksRingBufferId);  
}
export default run();