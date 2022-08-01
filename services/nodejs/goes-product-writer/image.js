import decoder from '@ucd-lib/goes-r-packet-decoder';
import {config, logger} from '@ucd-lib/casita-worker';
import path from 'path';
import send from './send.js';

const {apidUtils} = decoder;

async function handleImageMessage(metadata, payload, monitor, metric) {
  let product = apidUtils.get(metadata.apid);
  if( !product.imageScale && !product.label ) return;

  var dateObj = new Date(946728000000 + metadata.imagePayload.SECONDS_SINCE_EPOCH*1000);
  var [date, time] = dateObj.toISOString().split('T');
  time = time.replace(/\..*/, '');

  let productInfo = {
    satellite : config.satellite,
    product : (product.imageScale || product.label || 'unknown').toLowerCase().replace(/[^a-z0-9]+/g, '-'),
    date,
    hour : time.split(':')[0],
    minsec : time.split(':').splice(1,2).join('-'),
    band : product.band,
    apid : metadata.apid,
    x : metadata.imagePayload.UPPER_LOWER_LEFT_X_COORDINATE,
    y : metadata.imagePayload.UPPER_LOWER_LEFT_Y_COORDINATE
  }

  let basePath = path.resolve('/', 
    productInfo.satellite,
    productInfo.product,
    productInfo.date,
    productInfo.hour,
    productInfo.minsec,
    productInfo.band,
    productInfo.apid,
    'blocks',
    productInfo.x+'-'+productInfo.y
  );
  logger.debug('Sending image:  '+ basePath);

  if( metadata.rootMetadata ) {
    await send(
      productInfo,
      path.join(basePath, 'fragment-metadata.json'), 
      JSON.stringify(metadata)
    );
  } else {
    await send(
      productInfo,
      path.join(basePath, 'fragments', metadata.index+'', 'image-fragment-metadata.json'), 
      JSON.stringify(metadata)
    );
    await send(
      productInfo,
      path.join(basePath, 'fragments', metadata.index+'', 'image-fragment.jp2'), 
      payload
    );
  }

  monitor.setMaxMetric(
    metric.type,
     'apid', 
     Date.now() - dateObj.getTime(),
     {
      apid: metadata.apid,
      channel: metadata.streamName
    }
  );
}

export default handleImageMessage;