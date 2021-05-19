const path = require('path');
const fs = require('fs');
const Worker = require('../service/lib/worker');
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
    await this.insert(msg.data.command.file);
  }

  async addFromNfs(file) {
    if( !fs.existsSync(file) ) {
      logger.error('File does not exist: '+file);
      return;
    }

    let pathInfo = path.parsea(file);
  
    var [satellite, product, date, hour, minuteSecond, band, apid, blocks, blockXY] = pathInfo.path
      .replace(config.fs.nfsRoot, '')
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
  }

}

let worker = new BlockRingBufferWorker();
module.exports = worker;