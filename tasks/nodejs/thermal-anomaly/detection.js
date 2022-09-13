import {logger, pg, config, utils} from '@ucd-lib/casita-worker';
import fs from 'fs-extra';
import path from 'path';
import slack from './slack.js';

class EventDetection {

  constructor() {
    this.eventRadius = config.thermalAnomaly.eventRadius;
  }

  async addClassifiedPixels(roiBufferId, classifier) {
    if( !classifier ) classifier = config.thermalAnomaly.stddevClassifier;

    // grab all classified pixels
    let resp = await pg.query(
      `select * from ST_PixelOfValue(classify_thermal_anomaly($1, $2), 1);`,
      [roiBufferId, parseInt(classifier)]
    );

    let eventSet = {
      new : new Set(),
      continued : new Set()
    }

    // nothing to see here
    if( resp.rows.length === 0 ) {
      await this.closeOutEvents();
      return eventSet;
    }

    // grab block product info
    let presp = await pg.query(`SELECT 
        product, x, y, date, satellite, band, apid 
      FROM 
        roi_buffer 
      WHERE 
        roi_buffer_id = $1`, 
        [roiBufferId]
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
          eventSet.continued.add(await this.addToActiveThermalEvent(events, roiBufferId, info));
        } else {
          eventSet.new.add(await this.addCreateThermalEvent(roiBufferId, info));
        }
      } catch(e) {
        console.error(e);
      }
    }

    await this.closeOutEvents();

    eventSet.continued = Array.from(eventSet.continued);
    eventSet.new = Array.from(eventSet.new);

    eventSet.files = await this.createNFSProducts(eventSet, {roiBufferId, classifier});

    for( let eventId of eventSet.new ) {
      await slack(eventId);
    }

    return eventSet;
  }

  async closeOutEvents() {
    // close out any event over 48 hours old
    await pg.query(`
    UPDATE 
      thermal_anomaly_event
    SET 
      active = false
    FROM (
      WITH latest_px AS (
        SELECT 
          max(px.date), e.thermal_anomaly_event_id 
        FROM 
          thermal_anomaly_event_px px
        JOIN 
          thermal_anomaly_event e ON px.thermal_anomaly_event_id = e.thermal_anomaly_event_id
        WHERE 
          e.active = true
        GROUP BY 
          e.thermal_anomaly_event_id
      )
      SELECT 
        thermal_anomaly_event_id
      FROM 
        latest_px
      WHERE 
        max < (now() - interval '2 day')
    ) AS inactive_events
    WHERE 
      thermal_anomaly_event.thermal_anomaly_event_id = inactive_events.thermal_anomaly_event_id
    `);
  }

  async findActiveEvents(info) {
    // not sure why, but the pg lib doesn't seem to be casting the pixel x/y as int
    // so we have to specifically set them :/
    let resp = await pg.query(
      `select * from active_thermal_anomaly_events ate WHERE 
        roi = $1 AND
        x >= $2::INTEGER - $4::INTEGER AND x <= $2::INTEGER + $4::INTEGER AND
        y >= $3::INTEGER - $4::INTEGER AND y <= $3::INTEGER + $4::INTEGER`,
        [info.roi, info.pixel.x, info.pixel.y, this.eventRadius]
    );
    return resp.rows;
  }

  async addCreateThermalEvent(id, info) {
    logger.info('Creating thermal event for', id, info);

    let resp = await pg.query(`
    INSERT INTO 
      thermal_anomaly_event (created, product, roi, band)
    VALUES ($1, $2, $3, $4, $5) 
    RETURNING thermal_anomaly_event_id`, [
      info.date.toISOString(), info.product, info.roi, info.band
    ]);

    // TODO, create pixel

    await this.addToActiveThermalEvent(resp.rows, id, info);

    return resp.rows[0].thermal_anomaly_event_id;
  }

  async addToActiveThermalEvent(events, id, info) {
    // currently just use first event
    let event = Array.isArray(events) ? events[0] : events;
    let value = await this.getPxValue(id, info.pixel.x, info.pixel.y);

    logger.info('Adding thermal pixel for', event, info);

    let resp = await pg.query(`
      SELECT 
        thermal_anomaly_event_px_id 
      FROM 
        thermal_anomaly_event_px
      WHERE
        thermal_anomaly_event_id = $1 AND
        date = $2 AND
        block_x = $3 AND
        block_y = $4 AND
        pixel_x = $5 AND
        pixel_y = $6 AND
        classifier = $7`, 
      [event.thermal_anomaly_event_id, info.date.toISOString(), info.block.x, info.block.y, info.pixel.x, info.pixel.y, info.classifier]
    );

    if( !resp.rows.length ) {
      resp = await pg.query(`
      INSERT INTO thermal_anomaly_event_px (
        thermal_anomaly_event_id, date, block_x, block_y, pixel_x, pixel_y, classifier, value
      ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8
      ) RETURNING thermal_anomaly_event_px_id`, 
      [event.thermal_anomaly_event_id, info.date.toISOString(), info.block.x, info.block.y, info.pixel.x, info.pixel.y, info.classifier, value]);
    }

    // now check add pixels used;
    await this.addPxHistory(resp.rows[0].thermal_anomaly_event_px_id, info);

    return event.thermal_anomaly_event_id;
  }

  async addPxHistory(thermal_anomaly_event_px_id, pixel) {
    // get all stats product pixels for event
    let resp = await pg.query(
      `SELECT * FROM all_thermal_anomaly_px_values($1, $2, $3, $4, $5)`, 
      [pixel.product, pixel.block.x, pixel.block.y, pixel.pixel.x, pixel.pixel.y]
    );

    let productIdCache = {};

    for( let statsProduct of resp.rows ) {
      let productId = [statsProduct.satellite, statsProduct.band, statsProduct.product, statsProduct.apid].join('-');

      // get the product id for the pixel
      if( !productIdCache[productId] ) {
        productIdCache[productId] = await this.getOrCreatePxProduct(statsProduct);
      }
      statsProduct.thermal_anomaly_stats_product_id = productIdCache[productId];

      statsProduct.thermal_anomaly_stats_px_id = await this.getOrCreateStatsPx(pixel, statsProduct);


      try {
        await pg.query(`INSERT INTO 
            thermal_anomaly_event_px_stats_px (thermal_anomaly_event_px_id, thermal_anomaly_stats_px_id) 
          VALUES
            ($1, $2)`, 
          [thermal_anomaly_event_px_id, statsProduct.thermal_anomaly_stats_px_id]
        );
      } catch(e) {}
    }
  }

  async getOrCreateStatsPx(pixel, statsProduct) {
    // lookup the product id, create if needed
    let existsResp = await pg.query(`SELECT 
        thermal_anomaly_stats_px_id
      FROM 
        thermal_anomaly_stats_px 
      WHERE
        thermal_anomaly_stats_product_id = $1 AND
        date = $2 AND
        block_x = $3 AND 
        block_y = $4 AND
        pixel_x = $5 AND 
        pixel_y = $6
      `,
      [statsProduct.thermal_anomaly_stats_product_id, statsProduct.date, pixel.block.x, pixel.block.y, pixel.pixel.x, pixel.pixel.y]
    );

    if( !existsResp.rows.length ) {
      try {
        existsResp = await pg.query(`
          INSERT INTO 
            thermal_anomaly_stats_px (thermal_anomaly_stats_product_id, date, block_x, block_y, pixel_x, pixel_y, value) 
          VALUES
            ($1, $2, $3, $4, $5, $6, $7) RETURNING thermal_anomaly_stats_px_id`,
          [statsProduct.thermal_anomaly_stats_product_id, statsProduct.date, pixel.block.x, pixel.block.y, pixel.pixel.x, pixel.pixel.y, statsProduct.value]
        );
      } catch(e) {
        console.error(e);
      }
    }

    return existsResp.rows[0].thermal_anomaly_stats_px_id;
  }

  async getOrCreatePxProduct(statsProduct) {
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
      [statsProduct.satellite, statsProduct.band, statsProduct.product, statsProduct.apid]
    );

    if( !existsResp.rows.length ) {
      try {
        existsResp = await pg.query(`
          INSERT INTO 
            thermal_anomaly_stats_product (satellite, band, product, apid) 
          VALUES
            ($1, $2, $3, $4) RETURNING thermal_anomaly_stats_product_id`,
          [statsProduct.satellite, statsProduct.band, statsProduct.product, statsProduct.apid]
        );
      } catch(e) {
        console.error(e);
      }
    }

    return existsResp.rows[0].thermal_anomaly_stats_product_id;
  }

  async getPxValue(roi_buffer_id, x, y) {
    let resp = await pg.query(`
      SELECT ST_Value(rast, ${x}, ${y}) as value 
      FROM roi_buffer
      WHERE roi_buffer_id = $1`, [roi_buffer_id]);
    if( !resp.rows.length ) return -1;
    return resp.rows[0].value;
  }

  async createNFSProducts(eventSet, args) {
    if( eventSet.continued.length === 0 && eventSet.new.length === 0 ) {
      return;
    }

    let files = new Set(), pfiles;
    for( let event of eventSet.continued ) {
      pfiles = await this.createNFSProduct(event, args);
      pfiles.forEach(f => files.add(f));
    }
    for( let event of eventSet.new ) {
      pfiles = await this.createNFSProduct(event, args);
      pfiles.forEach(f => files.add(f));
    }

    return Array.from(files);
  }

  async createNFSProduct(eventId, args) {
    // get event
    let resp = await pg.query('SELECT * FROM thermal_anomaly_event WHERE thermal_anomaly_event_id = $1', [eventId]);
    let event = resp.rows[0];

    // get date, product
    resp = await pg.query('SELECT date, product, x, y FROM roi_buffer WHERE roi_buffer_id = $1', [args.roiBufferId]);
    let {date, product, x, y} = resp.rows[0];

    // get all pixels for date
    resp = await pg.query(`
      SELECT 
        px.*,
        ST_asGeoJson(ST_PixelAsPolygon(block.rast, px.pixel_x, px.pixel_y)) as geometry
      FROM 
        thermal_anomaly_event_px px,
        roi_buffer block
      WHERE
        px.thermal_anomaly_event_id = $1 AND
        px.date = $2 AND
        block.roi_buffer_id = $3`, 
      [eventId, date, args.roiBufferId]
    );
    
    let geojson = {
      type : 'FeatureCollection',
      features : []
    }

    for( let pixel of resp.rows ) {
      let pixelHistoryResp = await pg.query(
        'SELECT date, product, value FROM thermal_anomaly_stats_px_view WHERE thermal_anomaly_event_px_id = $1', 
        [pixel.thermal_anomaly_event_px_id]
      );

      let history = {};
      pixelHistoryResp.rows.forEach(item => {
        let date = item.date.toISOString();
        if( !history[date] ) history[date] = {};
        history[date][item.product.replace(/.*-hourly-max-?/, '') || 'hourly-max'] = item.value;
      })
      pixel.history = history;

      let feature = {
        type : 'Feature',
        geometry : JSON.parse(pixel.geometry),
      }
      delete pixel.geometry;
      feature.properties = pixel;
      
      geojson.features.push(feature);
    }

    // get prior hour
    
    let lastHour = new Date(date.toISOString().replace(/:.*/, ':00:00.000Z'));
    lastHour = new Date(lastHour.getTime() - (60 * 60 * 1000));

    // copy stats images
    let imageSrc = utils.getPathFromData({
      satellite : event.satellite,
      product,
      date : lastHour,
      band : event.band,
      apid : event.apid,
      x, y
    });

    let imageDst = utils.getPathFromData({
      satellite : event.satellite,
      product : config.thermalAnomaly.product,
      date : lastHour,
      band : event.band,
      apid : event.apid,
      x, y
    });

    let files = [];
    for( let taProduct of config.thermalAnomaly.products ) {
      await fs.mkdirp(path.join(config.fs.nfsRoot, imageDst));
      await fs.copyFile(
        path.join(config.fs.nfsRoot, imageSrc, taProduct+'.png'),
        path.join(config.fs.nfsRoot, imageDst, taProduct+'.png')
      );
      files.push(path.join(imageDst, taProduct+'.png'));
    }

    let geojsonPath = utils.getPathFromData({
      satellite : event.satellite,
      product : config.thermalAnomaly.product,
      date,
      band : event.band,
      apid : event.apid
    });

    await fs.mkdirp(path.join(config.fs.nfsRoot, geojsonPath));
    await fs.writeFile(
      path.join(config.fs.nfsRoot, geojsonPath, eventId+'-features.json'),
      JSON.stringify(geojson)
    );
    files.push(path.join(geojsonPath, eventId+'-features.json'));

    return files;
  }

}

const instance = new EventDetection();
function run() {
  return instance.addClassifiedPixels(
    config.roi_buffer_id || config.id,
    config.classifier
  );  
}
export default run;