const path = require('path');
const fs = require('fs');
const Worker = require('/service/lib/worker');
const {config, logger} = require('@ucd-lib/krm-node-utils');
const uuid = require('uuid');
const pg = require('./lib/pg');
const exec = require('./lib/exec');
const PRELOAD_TABLE_PREFIX = 'raster';
const BUFFER_SIZE = 10; // in days
const TABLE = 'public.blocks_ring_buffer';


class BlockRingBufferWorker extends Worker {

  constructor() {
    super();
    this.ensureSchema();
  }

  async ensureSchema() {
    await pg.connect();
    let schema = fs.readFileSync(path.join(__dirname, 'lib', 'schema.sql'), 'utf-8');
    await pg.query(schema);
  }

  async exec(msg) {
    let file = path.join(config.fs.nfsRoot, msg.data.ready[0].replace('file:///', ''));

    try {
      await this.addFromNfs(file);
    } catch(e) {
      logger.error(e);
    }
  }

  async addFromNfs(file) {
    if( !fs.existsSync(file) ) {
      logger.error('File does not exist: '+file);
      return;
    }
  
    var [satellite, product, date, hour, minuteSecond, band, apid, blocks, blockXY] = file
      .replace(config.fs.nfsRoot+'/', '')
      .split('/');

    let [x, y] = blockXY.split('-');
    var date = new Date(date+'T'+hour+':'+minuteSecond.replace('-', ':'));

    await this.insert(file, {satellite, product, date, band, apid, blocks, x, y});
  }

  async insert(file, meta) {
    if( !fs.existsSync(file) ) {
      logger.error('File does not exist: '+file);
      return;
    }

    await pg.connect();
    
    let preloadTable = PRELOAD_TABLE_PREFIX+'_'+uuid.v4().replace(/-/g, '_');
  
    logger.info(`Inserting ${file} into ${preloadTable}`);
    let {stdout} = await exec(`raster2pgsql ${file} ${preloadTable}`);
    await pg.query(stdout);

    let isoDate = meta.date.toISOString();
    let expire = new Date(meta.date.getTime() + (1000 * 60 * 60 * 24 * BUFFER_SIZE)).toISOString();
    
  
    await pg.query(`DELETE from ${TABLE} where expire <= $1`, [new Date().toISOString()]);
  
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
      limit 1`;
  
    await pg.query(cmd);
    await pg.query(`drop table ${preloadTable}`);

    await this.createProducts(meta.x, meta.y, meta.product);
    await pg.query(`DELETE from thermal_product where expire <= $1`, [new Date().toISOString()]);
  }

  /**
   * TODO: this SHOULD be in new worker
   */
  async createProducts() {
    await this.insertAverage(x, y, product);
    await this.insertClassified(x, y, product);
  }

  async insertAverage(x, y, product) {
    return pg.query(`WITH latestId AS (
      SELECT 
        MAX(blocks_ring_buffer_id) AS rid 
      FROM blocks_ring_buffer WHERE 
        band = 7 AND x = $1 AND y = $2 AND product = $3
    ),
    latest AS (
      SELECT 
        rast, blocks_ring_buffer_id AS rid, date, expire
        extract(hour from date ) as end, 
        extract(hour from date - interval '2 hour') as start
      FROM latestId, blocks_ring_buffer WHERE 
        blocks_ring_buffer_id = rid
    ),
    rasters AS (
      SELECT 
        rb.rast, blocks_ring_buffer_id AS rid 
      FROM blocks_ring_buffer rb, latest WHERE 
        band = 7 AND x = $1 AND y = $2 AND product = $3 AND 
        blocks_ring_buffer_id != latest.rid AND
        extract(hour from rb.date) >= latest.start AND
        extract(hour from rb.date) <= latest.end 
    ),
    avg AS (
      SELECT ST_Union(rast, 'MEAN') AS v FROM rasters	
    )
    INSERT INTO thermal_product (blocks_ring_buffer_id, product, expire, rast) 
      select ST_AsPNG(v, 1) as rast, 
      latest.expire as expire, 
      'average' as product,
      rid as blocks_ring_buffer_id 
    from avg, latest`, [x, y, product]);
  }

  insertClassified(x, y, product, stdev=20) {
    return pg.query(`with classified as (
      select * from b7_variance_detection($1, $2, $3, $4)
    )
    INSERT INTO thermal_product (blocks_ring_buffer_id, product, expire, rast)
    select 
      ST_AsPNG(rast, 1) as rast, 
      date, expire,
      'classified' as product,
      blocks_ring_buffer_id from classified`, [x, y, product, stdev]);
  }
}

let worker = new BlockRingBufferWorker();
worker.connect();
// module.exports = worker;