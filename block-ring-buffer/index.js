const path = require('path');
const fs = require('fs');
const Worker = require('/service/lib/worker');
const { config, logger } = require('@ucd-lib/krm-node-utils');
const uuid = require('uuid');
const pg = require('./lib/pg');
const exec = require('./lib/exec');
const EventDetection = require('./lib/detection');
const sendSlackMessage = require('./lib/slack');
const PRELOAD_TABLE_PREFIX = 'raster';
const BUFFER_SIZE = 10; // in days
const TABLE = 'public.blocks_ring_buffer';


class BlockRingBufferWorker extends Worker {

  constructor() {
    super();
    this.ensureSchema();
    this.detection = new EventDetection();
  }

  async ensureSchema() {
    await pg.connect();

    let schema = fs.readFileSync(path.join(__dirname, 'lib', 'sql', 'schema.sql'), 'utf-8');
    await pg.query(schema);

    schema = fs.readFileSync(path.join(__dirname, 'lib', 'sql', 'grouped-schema.sql'), 'utf-8');
    await pg.query(schema);
  }

  async exec(msg) {
    let file = path.join(config.fs.nfsRoot, msg.data.ready[0].replace('file:///', ''));

    try {
      await this.addFromNfs(file);
    } catch (e) {
      logger.error(e);
    }
  }

  async addFromNfs(file) {
    if (!fs.existsSync(file)) {
      logger.error('File does not exist: ' + file);
      return;
    }

    var [satellite, product, date, hour, minuteSecond, band, apid, blocks, blockXY] = file
      .replace(config.fs.nfsRoot + '/', '')
      .split('/');

    let [x, y] = blockXY.split('-');
    var date = new Date(date + 'T' + hour + ':' + minuteSecond.replace('-', ':'));

    await this.insert(file, { satellite, product, date, band, apid, blocks, x, y });
  }

  async insert(file, meta) {
    if (!fs.existsSync(file)) {
      logger.error('File does not exist: ' + file);
      return;
    }

    await pg.connect();

    let preloadTable = PRELOAD_TABLE_PREFIX + '_' + uuid.v4().replace(/-/g, '_');

    logger.info(`Inserting ${file} into ${preloadTable}`);
    let { stdout } = await exec(`raster2pgsql ${file} ${preloadTable}`);
    await pg.query(stdout);

    let isoDate = meta.date.toISOString();
    let expire = new Date(meta.date.getTime() + (1000 * 60 * 60 * 24 * BUFFER_SIZE)).toISOString();

    // try {
    //   await pg.query(`DELETE from thermal_product where expire <= $1 cascade`, [new Date().toISOString()]);
    // } catch(e) {}
    try {
      await pg.query(`DELETE from ${TABLE} where expire <= $1 cascade`, [new Date().toISOString()]);
    } catch (e) { }

    let cmd = `
    with temp as (
      select 
        rast
      from 
        ${preloadTable}
    )
    insert into ${TABLE}(date, x, y, satellite, product, apid, band, expire, rast) 
      select
        '${isoDate}' as date, 
        '${meta.x}' as x, 
        '${meta.y}' as y,
        '${meta.satellite}' as satellite,
        '${meta.product}' as product,
        '${meta.apid}' as apid,
        '${meta.band}' as band,
        '${expire}' as expire,
        rast 
      from temp 
      limit 1
      RETURNING blocks_ring_buffer_id`;

    let resp = await pg.query(cmd);
    await pg.query(`drop table ${preloadTable}`);
    let blocks_ring_buffer_id = resp.rows[0].blocks_ring_buffer_id;

    let priorHourDate = new Date(meta.date.getTime() - 1000 * 60 * 60);
    resp = await pg.query(`SELECT create_hourly_max('${meta.product}', ${meta.x}, ${meta.y}, '${priorHourDate.toISOString()}') as blocks_ring_buffer_grouped_id`);

    if (resp.rows[0].blocks_ring_buffer_grouped_id !== -1) {
      await pg.query(`SELECT create_thermal_grouped_products(${resp.rows[0].blocks_ring_buffer_grouped_id});`);
    }

    try {
      let eventSet = await this.detection.addClassifiedPixels(blocks_ring_buffer_id);
      let newEvents = Array.from(eventSet.new);
      for (let data of newEvents) {
        await sendSlackMessage(data);
      }
    } catch (e) {
      console.error(e);
    }

    await pg.query(`DELETE from blocks_ring_buffer_grouped where expire <= $1`, [new Date().toISOString()]);
    await pg.query(`DELETE from blocks_ring_buffer where expire <= $1`, [new Date().toISOString()]);
  }
}

let worker = new BlockRingBufferWorker();
worker.connect();
// module.exports = worker;