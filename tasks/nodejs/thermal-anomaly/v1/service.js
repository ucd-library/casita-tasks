const express = require('express')
const app = express();
const pg = require('./lib/pg');
const cors = require('cors');
const kml = require('./lib/kml');

process.on('unhandledRejection', e => {
  console.error('unhandledRejection, killing process shortly', e);
  setTimeout(() => process.exit(-1), 50);
});

app.use(cors());

pg.connect();


app.get('/_/thermal-anomaly/products', async (req, res) => {
  let resp = await pg.query(`SELECT date, x, y, satellite, product, apid, band, blocks_ring_buffer_id from blocks_ring_buffer`);
  res.json(resp.rows);
});

app.get('/_/thermal-anomaly/latest', async (req, res) => {
  let resp = await pg.query(`
    WITH latest AS (
      SELECT MAX(rb.date) as date, rb.x, rb.y, rb.satellite, rb.product, rb.apid, rb.band
      FROM blocks_ring_buffer_grouped rb
      GROUP BY rb.x, rb.y, rb.satellite, rb.product, rb.apid, rb.band
    )
    SELECT 
      latest.*, 
      rb.blocks_ring_buffer_grouped_id,
      '/_/thermal-anomaly/png/' || latest.product || '/' || latest.x || '/' || latest.y || '/' || to_char(latest.date , 'YYYY-MM-DD"T"HH24:MI:SS"') || '/[product]' as data_path
    FROM latest, blocks_ring_buffer_grouped rb
    WHERE rb.x = latest.x AND rb.y = latest.y AND rb.date = latest.date AND rb.product = latest.product`);
  res.json(resp.rows);
});

let types = ['amax-average', 'amax-stddev', 'amax-max', 'amax-min', 'average', 'min', 'max', 'stddev', 'raw']
app.get('/_/thermal-anomaly/png/:product/:x/:y/:date/:type', async (req, res) => {
  try {
    let product = req.params.product;
    let x = parseInt(req.params.x);
    let y = parseInt(req.params.y);
    let type = req.params.type;
    let date = req.params.date;
    let ratio = parseInt(req.query.ratio || 20);

    // if( date === 'raw' ) {
    //   let resp = await pg.query(`SELECT MAX(date) x, y, product, blocks_ring_buffer_id from blocks_ring_buffer group by x, y, satellite, product, apid, band`);
    //   let row = resp.rows.find(row => row.x === x && row.y === y && row.product === product);
    //   if( !row ) throw new Error(`Unable to find latest for x=${x} y=${y} product=${product}`);
    //   date = row.date;
    // }

    let resp;
    if (type === 'classified') {
      resp = await pg.query(`
        WITH image AS (
          SELECT blocks_ring_buffer_id FROM blocks_ring_buffer WHERE
          x = $1 AND y = $2 AND product = $3 AND date = $4 
        ),
        classifed AS (
          SELECT get_grouped_classified_product(image.blocks_ring_buffer_id, $5) as rast from image
        )
        SELECT 
          ST_AsPNG(rast, 1) AS png,
          image.blocks_ring_buffer_id AS blocks_ring_buffer_id
        FROM classifed, image
        `, [x, y, product, date, ratio]);
    } else if (type === 'raw') {
      resp = await pg.query(`
        WITH image AS (
          SELECT rast, blocks_ring_buffer_id FROM blocks_ring_buffer WHERE
          x = $1 AND y = $2 AND product = $3 AND date = $4 
        )
        SELECT 
          ST_AsPNG(rast, 1) AS png,
          image.blocks_ring_buffer_id AS blocks_ring_buffer_id
        FROM image`, [x, y, product, date]);
    } else if (types.includes(type)) {
      resp = await pg.query(`
        WITH image AS (
          SELECT blocks_ring_buffer_grouped_id, rast FROM blocks_ring_buffer_grouped WHERE
          x = $1 AND y = $2 AND product = $3 AND date = date_trunc('hour', $4::timestamp) AND type = $5
        )
        SELECT 
          ST_AsPNG(rast, 1) AS png,
          image.blocks_ring_buffer_grouped_id AS blocks_ring_buffer_grouped_id
        FROM image`, [x, y, product, date, type]);
    } else {
      throw new Error(`Unknown type provided '${type}', should be: classified, average, min, max or raw`);
    }

    if (!resp.rows.length) {
      throw new Error(`Unable to find: x=${x} y=${y} product=${product} type="${type}`);
    }

    resp = resp.rows[0];

    date = new Date(date).toISOString().replace(/(:)+/g, '-').replace(/\..*/, '');
    let name = type + '-' + product + '-' + x + '-' + y + '-' + date + '.png';

    res.set('Content-Disposition', `attachment; filename="${name}"`);
    res.set('Content-Type', 'application/png');
    res.set('Content-Length', resp.png.length);
    res.set('x-blocks-ring-buffer-id', resp.blocks_ring_buffer_id);
    res.set('Cache-control', 'public, max-age=21600')
    res.send(resp.png);


  } catch (e) {
    res.status(500).json({
      error: true,
      message: e.message,
      stack: e.stack
    });
  }
});


app.get('/_/thermal-anomaly/px-values/:product/:blockx/:blocky/:type/:pxx/:pxy', async (req, res) => {
  let resp;

  resp = await pg.query(
    `SELECT * FROM get_all_grouped_px_values($1, $2, $3, $4, $5, $6)`,
    [req.params.product, req.params.blockx, req.params.blocky,
    req.params.type, req.params.pxx, req.params.pxy]
  );

  res.json(resp.rows);
});

app.get('/_/thermal-anomaly/thermal-event-px/:id', async (req, res) => {
  let data = {};

  let resp = await pg.query(`select px.*, b.satellite, b.apid, b.product, b.band, b.x as block_x, b.y as block_y from thermal_event_px px
    left join goesr_raster_block b on b.goesr_raster_block_id = px.goesr_raster_block_id
    where thermal_event_px_id = $1
  `, [req.params.id])
  if (!resp.rows.length) {
    return res.status(500, { error: true, message: 'unknown thermal_event_px_id: ' + req.params.id });
  }
  data.pixel = resp.rows[0];

  resp = await pg.query(
    `select * from thermal_event_history where thermal_event_px_id = $1;`,
    [req.params.id]
  );

  if (resp.rows.length) {
    data.results = resp.rows;
    data.historical = true;
  } else {
    data.results = [];
    data.historical = false;
    let historyTypes = ['amax-average', 'amax-stddev', 'max'];
    for (let type of historyTypes) {
      resp = await pg.query(
        `SELECT * FROM get_all_grouped_px_values($1, $2, $3, $4, $5, $6)`,
        [data.pixel.product, data.pixel.block_x, data.pixel.block_y,
          type, data.pixel.pixel_x, data.pixel.pixel_y]
      );
      data.results = [...data.results, ...resp.rows];
    }
  }

  let tmp = {};
  data.results.forEach(item => {
    if (!tmp[item.date.toISOString()]) tmp[item.date.toISOString()] = { date: item.date.toISOString() };
    tmp[item.date.toISOString()][item.type] = item.value;
  });
  data.data = Object.values(tmp);
  delete data.results;

  res.json(data);
});

app.get('/_/thermal-anomaly/kml/data', async (req, res) => {
  let where = '';
  let params = [];
  let nameExtra = '';

  if (req.query.thermal_event_id) {
    where = 'AND thermal_event_id = $1';
    params.push(req.query.thermal_event_id);
    nameExtra = '-' + req.query.thermal_event_id;
  }

  let resp = await pg.query(
    `SELECT px.*, b.satellite, b.product, b.apid, b.band, b.x as block_x, b.y as block_y FROM thermal_event_px px, goesr_raster_block b 
    where b.goesr_raster_block_id = px.goesr_raster_block_id ${where}`,
    params
  );

  res.set('content-type', 'application/vnd.google-earth.kml+xml');
  res.set('Content-Disposition', `attachment; filename="thermal-events${nameExtra}.kml"`);
  res.send(kml(resp.rows));
});

app.get('/_/thermal-anomaly/kml/network', async (req, res) => {
  let param = '';
  let nameExtra = '';

  if (req.query.thermal_event_id) {
    nameExtra = '-network-' + req.query.thermal_event_id;
    param = '?thermal_event_id=' + req.query.thermal_event_id;
  }

  res.set('content-type', 'application/vnd.google-earth.kml+xml');
  res.set('Content-Disposition', `attachment; filename="thermal-events${nameExtra}.kml"`);
  res.send(`<?xml version="1.0" encoding="UTF-8"?> 
  <kml xmlns="http://earth.google.com/kml/2.0"> <Document>
  
  <NetworkLink> 
    <Url>
      <href>https://data.casita.library.ucdavis.edu/_/thermal-anomaly/kml/data${param}</href>
    </Url> 
    <refreshMode>onInterval</refreshMode> 
    <refreshInterval>600</refreshInterval> 
  </NetworkLink>
  
  </Document> </kml>`);
});




// app.get('/_/thermal-anomaly/px-values/grouped/:id/:x/:y', async (req, res) => {
//   let resp;

//   if( req.query.all === 'true' ) {
//     resp = await pg.query(
//       `SELECT * FROM get_all_blocks_px_values($1, $2, $3)`, 
//       [req.params.id, req.params.x, req.params.y]
//     );
//   } else {
//     resp = await pg.query(
//       `SELECT * FROM get_blocks_px_values($1, $2, $3)`,
//       [req.params.id, req.params.x, req.params.y]
//     );
//   }

//   res.json(resp.rows);
// });


app.listen(3000, () => console.log('ring-buffer-service listening on port 3000'));