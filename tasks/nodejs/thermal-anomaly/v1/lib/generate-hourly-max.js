const {Client} = require('pg');

class PG {

  constructor() {
    this.client = new Client({
      host : process.env.PG_HOST || 'postgres',
      user : process.env.PG_USERNAME || 'postgres',
      port : process.env.PG_PORT || 5432,
      database : process.env.PG_DATABASE || 'casita'
    });

    this.client.on('end', () => {
      this.connected = false;
    });
  }

  async connect() {
    if( this.connected ) return;

    if( this.connecting ) {
      await this.connecting;
    } else {
      this.connecting = this.client.connect();
      await this.connecting;
      this.connecting = null;
      this.connected = true;
    }
  }

  async disconnect() {
    if( !this.connected ) return;
    await this.client.disconnect();
    this.connected = false;
  }

  async query(query, params) {
    await this.connect();
    return this.client.query(query, params);
  }
}

let pg  = new PG();

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