import fs from 'fs-extra';
import path from 'path';
import { config, logger, pg, exec, utils } from '@ucd-lib/casita-worker';
import {v4} from 'uuid';

const TABLE = config.pg.roi.bufferTable;

async function insert(roi_buffer_id) {
  await pg.connect();

  let resp = await pg.query(`SELECT roi_buffer_id, roi_id as roi, product_id as product, band, date, expire from ${TABLE} where roi_buffer_id = $1`, [roi_buffer_id]);
  if( !resp.rows.length ) return {message: 'noop'};

  let meta = resp.rows[0];
  if( typeof meta.date === 'string' ) {
    meta.date = new Date(meta.date);
  }

  // generate all hourly max products and stats product for last hour
  resp = await pg.query(`SELECT create_hourly_max_stats(${roi_buffer_id}) as timestamp;`);  
  if( !resp.rows.length ) return meta;

  let timestamp = resp.rows[0].timestamp;
  if( typeof timestamp === 'string' ) {
    timestamp = new Date(timestamp);
  }

  // grab the ids
  let idsResp = await pg.query(`
    SELECT 
      product_id as product, roi_buffer_id 
    FROM 
      ${TABLE}
    WHERE 
      date = $1 AND
      band = $2 AND
      (
        product = '${meta.roi}-hourly-max' OR
        product = '${meta.roi}-hourly-max-10d-average' OR
        product = '${meta.roi}-hourly-max-10d-min' OR
        product = '${meta.roi}-hourly-max-10d-max' OR
        product = '${meta.roi}-hourly-max-10d-stddev'
      ) 
  `, [timestamp, meta.band]);

  meta.date = timestamp;
  let diskPath = path.resolve(config.fs.nfsRoot, utils.getPathFromData(meta));
  await fs.mkdirp(diskPath);

  meta.roi_buffer_ids = {};
  meta.files = [];

  // write files to disk
  let png = '', tiff = '', shortProductName = 0;
  for( let row of idsResp.rows ) {

    shortProductName = row.product.replace(meta.roi+'-', '');
    meta.roi_buffer_ids[shortProductName] = row.roi_buffer_id;
    png = path.resolve(diskPath, shortProductName+'.png');
    tiff = path.resolve(diskPath, shortProductName+'.tiff');
    meta.files.push(png);
    meta.files.push(tiff);
    
    let pngResp = await pg.query(`
      SELECT
        ST_asPNG(rast) as png,
        ST_AsTIFF(rast, 'LZW') as tiff
      FROM
        ${TABLE}
      WHERE
      roi_buffer_id = ${row.roi_buffer_id}
    `);
    
    await fs.writeFile(png, pngResp.rows[0].png);
    await fs.writeFile(tiff, pngResp.rows[0].tiff);
  }
  
  meta.roi_buffer_id = roi_buffer_id;

  return meta;
}

async function run() {
  return insert(config.roi_buffer_id || config.id);
}

export default run;