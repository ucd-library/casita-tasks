const pg = require('./pg');

(async function() {


  let r = await pg.query(`select 
  date_trunc('hour', date) as date,
  x, y, product
from blocks_ring_buffer`);
  let rows = r.rows;

  for( let row of rows ) {
    console.log(`create_hourly_max('${row.product}', ${row.x}, ${row.y}, '${row.date.toISOString()}')`);
    let result = await pg.query(`select create_hourly_max('${row.product}', ${row.x}, ${row.y}, '${row.date.toISOString()}')`);
    console.log(result);
  }
})()