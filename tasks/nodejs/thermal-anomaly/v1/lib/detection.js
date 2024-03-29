const pg = require('./pg');
const { logger } = require('@ucd-lib/krm-node-utils');

// const {Client} = require('pg');
// class PG {

//   constructor() {
//     this.client = new Client({
//       host : process.env.PG_HOST || 'postgres',
//       user : process.env.PG_USERNAME || 'postgres',
//       port : process.env.PG_PORT || 5432,
//       database : process.env.PG_DATABASE || 'casita'
//     });

//     this.client.on('end', () => {
//       this.connected = false;
//     });
//   }

//   async connect() {
//     if( this.connected ) return;

//     if( this.connecting ) {
//       await this.connecting;
//     } else {
//       this.connecting = this.client.connect();
//       await this.connecting;
//       this.connecting = null;
//       this.connected = true;
//     }
//   }

//   async disconnect() {
//     if( !this.connected ) return;
//     await this.client.disconnect();
//     this.connected = false;
//   }

//   async query(query, params) {
//     await this.connect();
//     return this.client.query(query, params);
//   }
// }

// let pg  = new PG();

class EventDetection {

  constructor() {
    this.groupRadius = 5;
    this.historyTypes = ['amax-average', 'amax-stddev', 'max'];
  }

  async addClassifiedPixels(id, classifier=4) {
    let resp = await pg.query(
      `select * from ST_PixelOfValue(get_grouped_classified_product($1, $2), 1);`,
      [id, classifier]
    );
    logger.info('  resp - ',  id, classifier, resp.rows);

    // todo get type
    let presp = await pg.query(`SELECT product, x, y, date, satellite, band, apid FROM blocks_ring_buffer where blocks_ring_buffer_id = ${id}`);
    let info = Object.assign({}, presp.rows[0]);

    let offsetX = 0, offsetY = 0;
    if( info.product === 'conus' ) {
      offsetX = 1462;
      offsetY = 422;
    }

    let eventSet = {
      new : new Set(),
      continued : new Set()
    }

    for( let pixel of resp.rows ) {
      info.block = {
        x : info.x,
        y : info.y
      };
      info.world = {
        x: info.x + pixel.x + offsetX,
        y: info.y + pixel.y + offsetY
      };
      info.pixel = {
        x : pixel.x, 
        y : pixel.y
      }

      try {
        let events = await this.findActiveEvents(info);
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
      `select * from active_thermal_events ate WHERE 
        ate.world_x >= ${info.world.x} - ${this.groupRadius} AND ate.world_x <= ${info.world.x} + ${this.groupRadius} AND
        ate.world_y >= ${info.world.y} - ${this.groupRadius} AND ate.world_y <= ${info.world.y} + ${this.groupRadius}`
    );
    return resp.rows;
  }

  async addCreateThermalEvent(id, info) {
    logger.info('Creating thermal event for', id, info);

    let resp = await pg.query(`INSERT INTO thermal_event (created)
    VALUES ('${info.date.toISOString()}') RETURNING thermal_event_id`);

    await this.addToActiveThermalEvent(resp.rows, id, info);

    return resp.rows[0];
  }

  async addToActiveThermalEvent(events, id, info) {
    // currently just use first event
    let event = Array.isArray(events) ? events[0] : events;
    let value = await this.getValue(id, info.pixel.x, info.pixel.y);

    logger.info('Adding thermal pixel for', event, info);

    let resp = await pg.query(`INSERT INTO thermal_event_px (
      thermal_event_id, goesr_raster_block_id, date, 
      world_x, world_y, pixel_x, pixel_y, value
    ) VALUES (
      ${event.thermal_event_id}, 
      get_block_product_id('${info.satellite}', '${info.product}', ${info.band}, ${info.block.x}, ${info.block.y}),
      '${info.date.toISOString()}', ${info.world.x}, ${info.world.y},
      ${info.pixel.x}, ${info.pixel.y}, ${value}
    ) RETURNING thermal_event_px_id`);

    // now check add pixels used;
    for( let type of this.historyTypes ) {
      try {
        await this.addPxHistory(resp.rows[0].thermal_event_px_id, info, type);
      } catch(e) {
        console.error(e);
      }
    }

    return event;
  }

  async addPxHistory(thermal_event_px_id, info, type) {
    let resp = await pg.query(
      `SELECT * FROM get_all_grouped_px_values($1, $2, $3, $4, $5, $6)`, 
      [info.product, info.block.x, info.block.y, 
        type, info.pixel.x, info.pixel.y]
    );

    for( let row of resp.rows ) {
      let existsResp = await pg.query(`SELECT * from thermal_event_px_product where
      date = $1 AND 
      goesr_raster_block_id = get_block_product_id($2, $4, $3, $5, $6)
      AND type = $7 AND pixel_x = $8 AND pixel_y = $9`,
      [row.date.toISOString(), info.satellite, info.band, info.product, info.block.x, info.block.y,
        type, info.pixel.x, info.pixel.y]);

      if( !existsResp.rows.length ) {
        try {
          existsResp = await pg.query(`INSERT INTO thermal_event_px_product 
          (date, goesr_raster_block_id, type, pixel_x, pixel_y, value) VALUES
          ($1, get_block_product_id($2, $3, $4, $5, $6), $7, $8, $9, $10) RETURNING thermal_event_px_product_id`,
          [
            row.date.toISOString(), 
            info.satellite, info.product, info.band, info.block.x, info.block.y,
            type, info.pixel.x, info.pixel.y, row.value
          ]);
        } catch(e) {
          console.error(e);
        }
      }

      try {
        await pg.query(`INSERT INTO thermal_event_px_history
        (thermal_event_px_id, thermal_event_px_product_id) VALUES
        ($1, $2)`, [thermal_event_px_id, existsResp.rows[0].thermal_event_px_product_id])
      } catch(e) {
        console.error(e);
      }
    }
  }

  async getValue(id, x, y) {
    let resp = await pg.query(`SELECT ST_Value(rast, ${x}, ${y}) as value FROM blocks_ring_buffer
    where blocks_ring_buffer_id = ${id}`);
    if( !resp.rows.length ) return -1;
    return resp.rows[0].value;
  }

}

module.exports = EventDetection;

// (async function() {
//   let worker = new EventCreation();

//   let resp = await pg.query(`select date, x, y, product, apid, band, satellite, blocks_ring_buffer_id from blocks_ring_buffer where date > '2021-08-19 12:00:00' order by date;`);
//   for( let row of resp.rows ) {
//     logger.info('Checking', row);
//     await worker.addClassifiedPixels(row.blocks_ring_buffer_id);
//   }
// })();

/**
 * st_asGEOJSON(
 *  st_transform(
 *    st_pixelAsPolygon(
 *      st_setscale(
 *         st_setupperleft(
 *            st_setsrid(rast,888897),
 *            833671.1918079999,
 *            4162343.907176
 *         ),
 *         501.004322*4
 *      ),
 *      141,
 *      126
 *    ),
 *   4269
 * )) 
 */