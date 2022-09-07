import {logger, pg, config, utils} from '@ucd-lib/casita-worker';

async function run() {
  return await pg.query(`
  insert into 
    roi_buffer(date,band,expire,roi_id,product_id,rast)
  select 
    date, 
    band, 
    date+'10 days'::interval, 
    roi_id,
    product_id,
    blocks_to_roi(date, band, roi_id, product_id)
  from
    (values ($1, $2, 'ca', $3))
  as 
    foo(date, band, roi_id, product_id)`, [config.datetime, config.band, config.product])
}

export default run;