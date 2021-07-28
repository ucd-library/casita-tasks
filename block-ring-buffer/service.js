const express = require('express')
const app = express();
const pg = require('./lib/pg');

pg.connect();


app.get('/_/thermal-anomaly/products', async (req, res) => {
  let resp = await pg.query(`SELECT date, x, y, satellite, product, apid, band, blocks_ring_buffer_id from blocks_ring_buffer`);
  res.json(resp.rows);
});

app.get('/_/thermal-anomaly/latest', async (req, res) => {
  let resp = await pg.query(`
    WITH latest AS (
      SELECT MAX(rb.date) as date, rb.x, rb.y, rb.satellite, rb.product, rb.apid, rb.band 
      FROM thermal_product tp 
      LEFT JOIN blocks_ring_buffer rb ON rb.blocks_ring_buffer_id = tp.blocks_ring_buffer_id
      GROUP BY rb.x, rb.y, rb.satellite, rb.product, rb.apid, rb.band
    )
    SELECT 
      latest.*, 
      rb.blocks_ring_buffer_id,
      '/_/thermal-anomaly/png/' || latest.product || '/' || latest.x || '/' || latest.y || '/' || to_char(latest.date , 'YYYY-MM-DD"T"HH24:MI:SS"') || '/[product]' as data_path
    FROM latest, blocks_ring_buffer rb
    WHERE rb.x = latest.x AND rb.y = latest.y AND rb.date = latest.date AND rb.product = latest.product`);
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
    res.set('Content-Length', resp.png.length);
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