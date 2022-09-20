import {logger, pg, config, utils} from '@ucd-lib/casita-worker';
import path from 'path';
import fs from 'fs-extra';

const PRODUCTS = config.pg.roi.products;

async function create(roiId) {
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
    (values ('${datetime.toISOString()}'::timestamp, ${config.band}, '${roiId}', '${config.product}'))
  as 
    foo(date, band, roi_id, product_id)
  returning roi_buffer_id;`);
}

async function run() {
  let resp;
  let ids = {
    [PRODUCTS.california3310.id] : '',
    [PRODUCTS.californiaGoes.id] : ''
  }

  try {
    resp = await create(PRODUCTS.california3310.id);
    ids[PRODUCTS.california3310.id] = resp.rows[0].roi_buffer_id;
  } catch(e) {
    resp = await pg.query(`select roi_buffer_id 
      from roi_buffer 
      where 
        date = '${config.datetime}'::timestamp and
        band = ${config.band} and
        roi_id = '${PRODUCTS.california3310.id}' and
        product_id = '${config.product}'
    `);
    ids[PRODUCTS.california3310.id] = resp.rows[0].roi_buffer_id;
  };

  try {
    resp = await create(PRODUCTS.californiaGoes.id);
    ids[PRODUCTS.californiaGoes.id] = resp.rows[0].roi_buffer_id;
  } catch(e) {
    resp = await pg.query(`select roi_buffer_id 
      from roi_buffer 
      where 
        date = '${config.datetime}'::timestamp and
        band = ${config.band} and
        roi_id = '${PRODUCTS.californiaGoes.id}' and
        product_id = '${config.product}'
    `);
    ids[PRODUCTS.californiaGoes.id] = resp.rows[0].roi_buffer_id;
  };

  let p = utils.getPathFromData({
    satellite : 'west',
    product : PRODUCTS.californiaGoes.product,
    date: config.datetime,
    band: config.band,
    apid : PRODUCTS.californiaGoes.apid,
  })

  let tiffFile = path.join(config.fs.nfsRoot, p, 'goes.tiff');
  let pngFile = path.join(config.fs.nfsRoot, p, 'wsg84.png');
  let wldFile = path.join(config.fs.nfsRoot, p, 'wsg84.wld');
  resp = await pg.query(`select 
      ST_Astiff(rast, 'LZW') as tiff,
      ST_AsPng(ST_Transform(rast, 3857)) as png,
      ST_GeoReference(ST_Transform(rast, 3857, 'ESRI')) as wld
    from roi_buffer 
    where roi_buffer_id = ${ids[PRODUCTS.californiaGoes.id]}
  `);

  await fs.mkdirp(path.join(config.fs.nfsRoot, p));
  console.log(tiffFile);
  await fs.writeFile(tiffFile, resp.rows[0].tiff);
  console.log(pngFile);
  await fs.writeFile(pngFile, resp.rows[0].png);
  console.log(pngFile);
  await fs.writeFile(wldFile, resp.rows[0].wld);

  return {
    satellite : 'west',
    product : PRODUCTS.californiaGoes.product,
    date: config.datetime,
    band: config.band,
    apid : PRODUCTS.californiaGoes.apid,
    files : [tiffFile, pngFile, wldFile],
    roi_buffer_ids : ids
  }
}

export default run;