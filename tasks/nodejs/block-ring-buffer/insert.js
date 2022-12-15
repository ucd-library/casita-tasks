import fs from 'fs';
import { config, logger, pg, exec, utils } from '@ucd-lib/casita-worker';
import {v4} from 'uuid';

const PRELOAD_TABLE_PREFIX = config.pg.ringBuffer.preloadTablePrefix;
const BUFFER_SIZE = config.pg.ringBuffer.sizes;
const DEFAULT_BUFFER_SIZE = config.pg.ringBuffer.defaultSize;
const TABLE = config.pg.ringBuffer.table;
const PRELOAD_TABLE_LIST = 'blocks_ring_buffer_preload_tables';

class BlockRingBuffer {

  exec(file) {
    try {
      return this.addFromNfs(file);
    } catch (e) {
      throw new Error('failed to insert into block ring buffer: '+ file +', '+e.message);
    }
  }

  async addFromNfs(file) {
    if (!fs.existsSync(file)) {
      throw new Error('File does not exist: ' + file);      
    }


    let fileData = utils.getDataFromPath(file);
    fileData.file = file;
    fileData.blocks_ring_buffer_id = await this.insert(file, fileData);

    return fileData;
  }

  getBufferSize(meta) {
    if( process.env.BUFFER_SIZE ) {
      return parseFloat(process.env.BUFFER_SIZE);
    }
    for( let size of BUFFER_SIZE ) {
      if( meta[size.key]+'' === size.value+'' ) {
        return size.size;
      }
    }
    return DEFAULT_BUFFER_SIZE;
  }

  async insert(file, meta) {
    await pg.connect();

    let preloadTable = PRELOAD_TABLE_PREFIX + '_' + v4().replace(/-/g, '_');

    logger.debug(`Inserting ${file} into ${preloadTable}`);
    let {stdout} = await exec(`raster2pgsql ${file} ${preloadTable}`);
    let resp = await pg.query(stdout);
    logger.debug(resp);

    await pg.query(`INSERT INTO ${PRELOAD_TABLE_LIST} (table_name) VALUES ($1)`, [preloadTable])

    if( !(await this.sanityCheck(preloadTable, meta)) ) {
      logger.warn('Sanity check failed for', meta);
      try {
        await pg.query(`drop table ${preloadTable}`);
        await pg.query(`DELETE from ${PRELOAD_TABLE_LIST} WHERE table_name = $1`, [preloadTable]);
      } catch(e) {}
      if( config.pg.disconnectAfterExec === true ) {
        await pg.end();
      }
      return -1;
    } 

    let isoDate = meta.datetime.toISOString();
    // buffer size is in days
    let buffer = this.getBufferSize(meta);
    let expire = new Date(meta.datetime.getTime() + Math.ceil(1000 * 60 * 60 * 24 * buffer)).toISOString();

    try {
      await pg.query(`DELETE from ${TABLE} cascade where expire <= now()`);
    } catch (e) { }

    try {
      await pg.query(`DELETE from ${TABLE} where date = '${isoDate}' and
      '${meta.x}' = x and
      '${meta.y}' = y and
      '${meta.satellite}' = satellite and
      '${meta.product}' = product and
      '${meta.apid}' = apid and
      '${meta.band}' = band`);
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

    resp = await pg.query(cmd);
    let blocks_ring_buffer_id = resp.rows[0].blocks_ring_buffer_id;
    
    try {
      await pg.query(`drop table ${preloadTable}`);
      await pg.query(`DELETE from ${PRELOAD_TABLE_LIST} WHERE table_name = $1`, [preloadTable])
    } catch(e) {
      logger.error(e);
    }
    
    if( config.pg.disconnectAfterExec === true ) {
      await pg.end();
    }

    return blocks_ring_buffer_id;
  }

  async sanityCheck(table, metadata) {
    // todo; change based on band
    let sanityCheckValue = 8000;

    let resp = await pg.query(`
      with rast as (
        select 
          ST_Reclass(rast, 1,'[0-${sanityCheckValue-1}]:0,[${sanityCheckValue}-100000):1','4BUI', 0) as rast 
        from 
          ${table} 
        limit 1
      ),
      classified as (
        select ST_PixelOfValue(rast.rast, 1) from rast
      )
      select count(*) as count from classified`
    );

    let above = resp.rows[0].count;

    resp = await pg.query(`select st_count(rast) as count from ${table}`);
    let total = resp.rows[0].count;

    return ((above / total) < 0.5);
  }
}

async function run() {
  let instance = new BlockRingBuffer();
  let response = await instance.exec(config.file);
  return response;
}
export default run;