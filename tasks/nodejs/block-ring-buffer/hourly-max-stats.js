import fs from 'fs';
import { config, logger, pg, exec, utils } from '@ucd-lib/casita-worker';
import {v4} from 'uuid';

const TABLE = config.pg.ringBuffer.table;


async function insert(blocks_ring_buffer_id) {
  await pg.connect();

  let resp = await pg.query(`SELECT product, x, y, date, satellite, apid, band, expire from ${TABLE} where blocks_ring_buffer_id = $1`, [blocks_ring_buffer_id]);
  if( !resp.rows.length ) return {message: 'noop'};

  let meta = resp.rows[0];
  if( typeof meta.data === 'string' ) {
    meta.date = new Date(meta.date);
  }

  let priorHourDate = new Date(meta.date.getTime() - 1000 * 60 * 60);
  resp = await pg.query(`SELECT create_all_hourly_max('${meta.product}', ${meta.x}, ${meta.y}, '${priorHourDate.toISOString()}') as blocks_ring_buffer_id`);

  await pg.query(`SELECT create_hourly_max_stats(${resp.rows[0].blocks_ring_buffer_id});`);
  
// console.log(`
// WITH hmax AS (
//   SELECT date FROM blocks_ring_buffer WHERE blocks_ring_buffer_id = $1
// )
// SELECT product, blocks_ring_buffer FROM ${TABLE} tbl WHERE tbl.date = hmax.date AND
// (product = "${meta.product}-hourly-max-10d-average" OR
// product = "${meta.product}-hourly-max-10d-min" OR
// product = "${meta.product}-hourly-max-10d-max" OR
// product = "${meta.product}-hourly-max-10d-stddev") 
// `, resp.rows[0].blocks_ring_buffer_id);
  // grab the ids
  let idsResp = await pg.query(`
    WITH hmax AS (
      SELECT date FROM blocks_ring_buffer WHERE blocks_ring_buffer_id = $1
    )
    SELECT product, blocks_ring_buffer_id FROM ${TABLE} tbl, hmax WHERE tbl.date = hmax.date AND
    (product = '${meta.product}-hourly-max-10d-average' OR
    product = '${meta.product}-hourly-max-10d-min' OR
    product = '${meta.product}-hourly-max-10d-max' OR
    product = '${meta.product}-hourly-max-10d-stddev') 
  `, [resp.rows[0].blocks_ring_buffer_id]);

  meta.pgHStatsIds = {
    'hourly-max' : resp.rows[0].blocks_ring_buffer_id
  };

  idsResp.rows.forEach(row => {
    meta.blocks_ring_buffer_ids[row.product.replace(meta.product+'-', '')] = row.blocks_ring_buffer_id;
  });

  meta.blocks_ring_buffer_id = blocks_ring_buffer_id;

  return meta;
}

async function run() {
  return insert(config.blocks_ring_buffer_id || config.id);
}

export default run;