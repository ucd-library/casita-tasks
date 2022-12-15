// NOTE: this is to cleaup bad tmp tables
import pg from 'pg';
const {Pool} = pg;

let client = new Pool({
  host : 'localhost', 
  user : 'postgres', 
  port : 5432,
  database : 'casita',
  options : '--search_path=public,roi',
  max : 3
});

let query = `SELECT *
FROM pg_catalog.pg_tables
WHERE schemaname != 'pg_catalog' AND 
    schemaname != 'information_schema'`;

let resp = await client.query(query);
for( let row of resp.rows ) {
  console.log(row.tablename);
  if( row.tablename.match(/^raster_[a-z0-9]{8}_/) ) {
    console.log('  droping '+row.tablename);
    await client.query('drop table '+row.tablename);
  }
}