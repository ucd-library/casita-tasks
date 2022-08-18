import fs from 'fs-extra';
import path from 'path';
import { config, logger, pg, exec, utils } from '@ucd-lib/casita-worker';
import {v4} from 'uuid';

const TABLE = config.pg.ringBuffer.table;


async function insert(blocks_ring_buffer_id) {
  await pg.connect();

  let resp = await pg.query(`SELECT product, x, y, date, satellite, apid, band, expire from ${TABLE} where blocks_ring_buffer_id = $1`, [blocks_ring_buffer_id]);
  if( !resp.rows.length ) return {message: 'noop'};

  let meta = resp.rows[0];
  if( typeof meta.date === 'string' ) {
    meta.date = new Date(meta.date);
  }

  // generate all hourly max products and stats product for last hour
  resp = await pg.query(`SELECT create_hourly_max_stats(${blocks_ring_buffer_id}) as timestamp;`);  
  if( !resp.rows.length ) return meta;

  let timestamp = resp.rows[0].timestamp;
  if( typeof timestamp === 'string' ) {
    timestamp = new Date(timestamp);
  }

  // grab the ids
  let idsResp = await pg.query(`
    SELECT 
      product, blocks_ring_buffer_id 
    FROM 
      ${TABLE}
    WHERE 
      date = $1 AND
      band = $2 AND
      x = $3 AND 
      y = $4 AND
      (
        product = '${meta.product}-hourly-max' OR
        product = '${meta.product}-hourly-max-10d-average' OR
        product = '${meta.product}-hourly-max-10d-min' OR
        product = '${meta.product}-hourly-max-10d-max' OR
        product = '${meta.product}-hourly-max-10d-stddev'
      ) 
  `, [timestamp, meta.band, meta.x, meta.y]);

  meta.date = timestamp;
  let diskPath = path.resolve(config.fs.nfsRoot, utils.getPathFromData(meta));
  await fs.mkdirp(diskPath);

  meta.blocks_ring_buffer_ids = {};
  meta.files = [];

  // write files to disk
  let file = '', shortProductName = 0;
  for( let row of idsResp.rows ) {

    shortProductName = row.product.replace(meta.product+'-', '');
    meta.blocks_ring_buffer_ids[shortProductName] = row.blocks_ring_buffer_id;
    file = path.resolve(diskPath, shortProductName+'.png');
    meta.files.push(file);

    // TODO: should we always override?
    // can we detect what updated?
    if( fs.existsSync(file) ) continue;
    
    let pngResp = await pg.query(`
      SELECT
        ST_asPNG(rast) as rast
      FROM
        ${TABLE}
      WHERE
      blocks_ring_buffer_id = ${row.blocks_ring_buffer_id}
    `);
    
    await fs.writeFile(file, pngResp.rows[0].rast);
  }
  
  meta.blocks_ring_buffer_id = blocks_ring_buffer_id;

  return meta;
}

async function run() {
  return insert(config.blocks_ring_buffer_id || config.id);
}

export default run;