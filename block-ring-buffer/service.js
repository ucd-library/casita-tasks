const express = require('express')
const app = express();
const pg = require('./lib/pg');

pg.connect();


function getClassified(x, y, product, stdev=20) {
  return pg.query(`with classified as (
    select b7_variance_detection($1, $2, $3, $4)
),
select ST_AsPNG(v, 1) as png from classified`, [x, y, product, stdev]);
}

function getAverage(x, y, product) {
  return pg.query(`WITH latestId AS (
    SELECT 
      MAX(blocks_ring_buffer_id) AS rid 
    FROM blocks_ring_buffer WHERE 
      band = 7 AND x = $1 AND y = $2 AND product = $3
  ),
  latest AS (
    SELECT 
      rast, blocks_ring_buffer_id AS rid, date,
      extract(hour from date ) as end, 
      extract(hour from date - interval '2 hour') as start
    FROM latestId, blocks_ring_buffer WHERE 
      blocks_ring_buffer_id = rid
  ),
  rasters AS (
    SELECT 
      rb.rast, blocks_ring_buffer_id AS rid 
    FROM blocks_ring_buffer rb, latest WHERE 
      band = 7 AND x = x_in AND y = y_in AND product = product_in AND 
      blocks_ring_buffer_id != latest.rid AND
      extract(hour from rb.date) >= latest.start AND
      extract(hour from rb.date) <= latest.end 
  ),
  avg AS (
    SELECT ST_Union(rast, 'MEAN') AS v FROM rasters	
  ),
  select ST_AsPNG(v, 1) as png from avg`, [x, y, product]);
}

function getLatest(x, y, product) {
  return pg.query(`WITH latestId AS (
    SELECT 
      MAX(blocks_ring_buffer_id) AS rid 
    FROM blocks_ring_buffer WHERE 
      band = 7 AND x = $1 AND y = $2 AND product = $3
  ),
  latest AS (
    SELECT 
      rast, blocks_ring_buffer_id AS rid, date
    FROM latestId, blocks_ring_buffer WHERE 
      blocks_ring_buffer_id = rid
  ),
  select ST_AsPNG(rast, 1) as png from latest`, [x, y, product]);
}

app.get('/_/thermal-anomaly/png/:product/:x/:y/type', async (req, res) => {
  try {
    let product = req.params.channel;
    let x = req.params.x;
    let y = req.params.y;
    let type = req.params.type;

    let resp;
    if( type === 'classified' ) {
      resp = await getClassified(x, y, product, req.query.stdev);
    } else if( type === 'average' ) {
      resp = await getAverage(x, y, product, req.query.stdev);
    } else if( type === 'latest' ) {
      resp = await getLatest(x, y, product, req.query.stdev);
    } else {
      throw new Error(`Unknown type provided '${type}', should be: classified, average or current`);
    }

    resp = resp.rows[0];

    let name = type+'.png';
    if( resp.date ) {
      name = resp.date.toISOString().replace(/(:)+/g, '-').replace(/\..*/, '')+'-'+name;
    }
    if( resp.blocks_ring_buffer_id ) {
      name = blocks_ring_buffer_id+'-'+name;
    }


    res.set(`Content-Disposition: attachment; filename="${name}"`);
    res.set('Content-Type: application/png');
    res.send(resp.rows[0].png);
    

  } catch(e) {
    res.status(500).json({
      error : true,
      message : e.message,
      stack : e.stack
    });
  }
});