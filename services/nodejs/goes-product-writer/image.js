import decoder from '@ucd-lib/goes-r-packet-decoder';
import {config, logger} from '@ucd-lib/casita-worker';
import path from 'path';
import send from './send.js';

const {apidUtils} = decoder;

async function handleImageMessage(metadata, payload, monitor, metric) {
  let product = apidUtils.get(metadata.apid);
  if( !product.imageScale && !product.label ) return;

  var dataObj = new Date(946728000000 + metadata.imagePayload.SECONDS_SINCE_EPOCH*1000);
  var [date, time] = dataObj.toISOString().split('T');
  time = time.replace(/\..*/, '');

  let basePath = path.resolve('/', 
    config.satellite,
    (product.imageScale || product.label || 'unknown').toLowerCase().replace(/[^a-z0-9]+/g, '-'),
    date,
    time.split(':')[0],
    time.split(':').splice(1,2).join('-'),
    product.band,
    metadata.apid,
    'blocks',
    metadata.imagePayload.UPPER_LOWER_LEFT_X_COORDINATE+'-'+metadata.imagePayload.UPPER_LOWER_LEFT_Y_COORDINATE
  );
  logger.debug('Sending image:  '+ basePath);

  if( metadata.rootMetadata ) {
    await send(
      path.join(basePath, 'fragment-metadata.json'), 
      JSON.stringify(metadata)
    );
  } else {

    await send(
      path.join(basePath, 'fragments', metadata.index+'', 'image-fragment-metadata.json'), 
      JSON.stringify(metadata)
    );

    await send(
      path.join(basePath, 'fragments', metadata.index+'', 'image-fragment.jp2'), 
      payload
    );
  }

  monitor.setMaxMetric(
    metric.type,
     'channel', 
     Date.now() - new Date(metadata.time).getTime(),
     {
      apid: metadata.apid,
      channel: metadata.streamName
    }
  );
}

export default handleImageMessage;