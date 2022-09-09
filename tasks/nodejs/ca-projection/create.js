import {logger, pg, config, utils} from '@ucd-lib/casita-worker';

async function run() {
  let datetime = config.datetime;
  if( typeof datetime === 'string' ) datetime = new Date(datetime);
  
  return await pg.query(`
  insert into 
    roi_buffer(date,band,expire,roi_id,product_id,rast)
  select 
    date, 
    band, 
    date +'10 days'::interval, 
    roi_id,
    product_id,
    blocks_to_roi(date, band, roi_id, product_id)
  from
    (values ('${datetime.toISOString()}'::timestamp, ${config.band}, 'ca', '${config.product}'))
  as 
    foo(date, band, roi_id, product_id);`)
}

export default run;