const express = require('express')
const app = express();
const pg = require('./lib/pg');

pg.connect();


function getClassified(x, y, product, stdev=20) {
  return pg.query(`with classified as (
    select * from b7_variance_detection($1, $2, $3, $4)
)
select ST_AsPNG(rast, 1) as png, date, blocks_ring_buffer_id from classified`, [x, y, product, stdev]);
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
      band = 7 AND x = $1 AND y = $2 AND product = $3 AND 
      blocks_ring_buffer_id != latest.rid AND
      extract(hour from rb.date) >= latest.start AND
      extract(hour from rb.date) <= latest.end 
  ),
  avg AS (
    SELECT ST_Union(rast, 'MEAN') AS v FROM rasters	
  )
  select ST_AsPNG(v, 1) as png, date, rid as blocks_ring_buffer_id from avg, latest`, [x, y, product]);
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
      rast, blocks_ring_buffer_id, date
    FROM latestId, blocks_ring_buffer WHERE 
      blocks_ring_buffer_id = rid
  )
  select ST_AsPNG(rast, 1) as png, date, blocks_ring_buffer_id from latest`, [x, y, product]);
}

app.get('/_/thermal-anomaly/products', async (req, res) => {
  let resp = await pg.query(`SELECT date, x, y, satellite, product, apid, band from blocks_ring_buffer`);
  res.json(resp.rows);
});

app.get('/_/thermal-anomaly/latest', async (req, res) => {
  let resp = await pg.query(`SELECT MAX(date) as date, x, y, satellite, product, apid, band from blocks_ring_buffer group by x, y, satellite, product, apid, band`);
  res.json(resp.rows);
});

let types = ['average', 'min', 'max', 'stddev']
app.get('/_/thermal-anomaly/png/:product/:x/:y/:date/:type', async (req, res) => {
  try {
    let product = req.params.product;
    let x = parseInt(req.params.x);
    let y = parseInt(req.params.y);
    let type = req.params.type;
    let date = req.params.date;
    let ratio = parseInt(req.query.ratio || 20);

    pg.query(`SELECT * from `, [x, y, date, product]);

    if( date === 'latest' ) {
      let resp = await pg.query(`SELECT MAX(date) x, y, product, blocks_ring_buffer_id from blocks_ring_buffer group by x, y, satellite, product, apid, band`);
      let row = resp.rows.find(row => row.x === x && row.y === y && row.product === product);
      if( !row ) throw new Error(`Unable to find latest for x=${x} y=${y} product=${product}`);
      date = row.date;
    }

    let resp;
    if( type === 'classified' ) {
      resp = await pg.query(`
        WITH image AS (
          SELECT blocks_ring_buffer_id FROM blocks_ring_buffer WHERE
          x = $1 AND y = $2 AND product = $3 AND date = $4 
        ),
        classifed AS (
          SELECT get_thermal_classified_product(image.blocks_ring_buffer_id, $5) as rast from image
        )
        SELECT 
          ST_AsPNG(rast, 1) AS png 
        FROM classifed
        `, [x, y, product, date, ratio]);
    } else if( types.includes(type)  ) {
      resp = await pg.query(`
        WITH image AS (
          SELECT blocks_ring_buffer_id FROM blocks_ring_buffer WHERE
          x = $1 AND y = $2 AND product = $3 AND date = $4 
        )
        SELECT 
          ST_AsPNG(rast, 1) AS png 
        FROM thermal_product tp, image
        WHERE tp.blocks_ring_buffer_id = image.blocks_ring_buffer_id AND
        tp.product = $5`, [x, y, product, date, type]);
    } else {
      throw new Error(`Unknown type provided '${type}', should be: classified, average or current`);
    }

    if( !resp.rows.length ) {
      throw new Error(`Unable to find: x=${x} y=${y} product=${product} type="${type}`);
    }

    resp = resp.rows[0];

    date = new Date(date).toISOString().replace(/(:)+/g, '-').replace(/\..*/, '');
    let name = type+'-'+product+'-'+x+'-'+y+'-'+date+'.png';

    res.set('Content-Disposition', `attachment; filename="${name}"`);
    res.set('Content-Type', 'application/png');
    res.send(resp.png);
    

  } catch(e) {
    res.status(500).json({
      error : true,
      message : e.message,
      stack : e.stack
    });
  }
});

app.listen(3000, () => console.log('ring-buffer-service listening on port 3000'));