const pg = require('./pg');

(async function() {


//   let r = await pg.query(`select 
//   date_trunc('hour', date) as date,
//   x, y, product
// from blocks_ring_buffer`);
//   let rows = r.rows;

//   for( let row of rows ) {
//     console.log(`create_hourly_max('${row.product}', ${row.x}, ${row.y}, '${row.date.toISOString()}')`);
//     let result = await pg.query(`select create_hourly_max('${row.product}', ${row.x}, ${row.y}, '${row.date.toISOString()}')`);
//     console.log(result);
//   }

  let r = await pg.query(`select 
  blocks_ring_buffer_grouped_id
from blocks_ring_buffer_grouped where type = 'max' order by date`);
  let rows = r.rows;

  for( let row of rows ) {
    console.log(row);
    try {
    let result = await pg.query(`select create_thermal_grouped_products('${row.blocks_ring_buffer_grouped_id}')`);
    console.log(result);
    } catch(e) {
      console.error(e);
    }
  }
})()