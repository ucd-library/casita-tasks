import fs from 'fs';
import { config, logger, pg, exec, utils } from '@ucd-lib/casita-worker';
import {v4} from 'uuid';

const PRELOAD_TABLE_PREFIX = config.pg.ringBuffer.preloadTablePrefix;
const BUFFER_SIZE = config.pg.ringBuffer.sizes;
const DEFAULT_BUFFER_SIZE = config.pg.ringBuffer.defaultSize;
const TABLE = config.pg.ringBuffer.table;

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
    for( let size of BUFFER_SIZE ) {
      if( meta[size.key]+'' === size.value+'' ) {
        return {query: `${size.key} = '${size.value}'`, size: size.size};
      }
    }
    return {query: '', size: DEFAULT_BUFFER_SIZE};
  }

  async insert(file, meta) {
    await pg.connect();

    let preloadTable = PRELOAD_TABLE_PREFIX + '_' + v4().replace(/-/g, '_');

    logger.debug(`Inserting ${file} into ${preloadTable}`);
    let {stdout} = await exec(`raster2pgsql ${file} ${preloadTable}`);
    let resp = await pg.query(stdout);
    logger.debug(resp);

    let isoDate = meta.datetime.toISOString();
    // buffer size is in days
    let buffer = this.getBufferSize(meta);
    let expire = new Date(meta.datetime.getTime() + (1000 * 60 * 60 * 24 * buffer.size)).toISOString();

    try {
      if( buffer.query ) buffer.query = buffer.query+' and '
      await pg.query(`DELETE from ${TABLE} where ${buffer.query} expire <= $1 cascade`, [new Date().toISOString()]);
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
    logger.info(resp);

    let blocks_ring_buffer_id, priorHourDate;
    try {
      await pg.query(`drop table ${preloadTable}`);
      blocks_ring_buffer_id = resp.rows[0].blocks_ring_buffer_id;
    } catch(e) {
      logger.error(e);
    }
    
    if( config.pg.disconnectAfterExec === true ) {
      await pg.end();
    }

    return blocks_ring_buffer_id;
  }
}

async function run() {
  let instance = new BlockRingBuffer();
  let response = await instance.exec(config.file);
  return response;
}
export default run;