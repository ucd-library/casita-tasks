import {logger, pg, config, utils} from '@ucd-lib/casita-worker';
import path from 'path';
import fs from 'fs-extra';

async function create() {
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
    foo(date, band, roi_id, product_id)
  returning roi_buffer_id;`);
}

async function run() {
  let roi_buffer_id;
  try {
    roi_buffer_id = await create();
    roi_buffer_id = roi_buffer_id.rows[0].roi_buffer_id
  } catch(e) {
    roi_buffer_id = await pg.query(`select roi_buffer_id 
      from roi_buffer 
      where 
        date = '${config.datetime}'::timestamp and
        band = ${config.band} and
        roi_id = 'ca' and
        product_id = '${config.product}'
    `);
    roi_buffer_id = roi_buffer_id.rows[0].roi_buffer_id;
  };

  let p = utils.getPathFromData({
    satellite : 'west',
    product : 'california',
    date: config.datetime,
    band: config.band,
    apid : 'imagery'
  })

  let tiffFile = path.join(config.fs.nfsRoot, p, 'image.tiff');
  let pngFile = path.join(config.fs.nfsRoot, p, 'image.png');
  let resp = await pg.query(`select 
      ST_Astiff(rast) as tiff,
      ST_AsPng(rast) as png
    from roi_buffer 
    where roi_buffer_id = ${roi_buffer_id}
  `);

  await fs.mkdirp(path.join(config.fs.nfsRoot, p));
  await fs.writeFile(tiffFile, resp.rows[0].tiff);
  await fs.writeFile(pngFile, resp.rows[0].png);

  return {
    satellite : 'west',
    product : 'california',
    date: config.datetime,
    band: config.band,
    apid : 'imagery',
    files : [tiffFile, pngFile],
    roi_buffer_id
  }
}

export default run;