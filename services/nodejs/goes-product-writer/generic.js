import decoder from '@ucd-lib/goes-r-packet-decoder';
import {config, logger} from '@ucd-lib/casita-worker';
import path from 'path';
import send from './send.js';

const {apidUtils} = decoder;

async function handleGenericMessage(metadata, payload, monitor, metric) {
  var dateObj = new Date(946728000000 + metadata.headers.SECONDS_SINCE_EPOCH*1000);
  var [date, time] = dateObj.toISOString().split('T');
  time = time.replace(/\..*/, '');

  let product = apidUtils.get(metadata.apid);
  let ms = false;
  if( metadata.spacePacketHeaders && metadata.spacePacketHeaders && 
    metadata.spacePacketHeaders.secondary ) {
    ms = metadata.spacePacketHeaders.secondary.MILLISECONDS_OF_THE_DAY+'';
  }
  let basePath;

  let productName = (product.imageScale || product.label || 'unknown').toLowerCase().replace(/[^a-z0-9]+/g, '-');

  let productInfo = {
    satellite : config.satellite,
    product : (product.imageScale || product.label || 'unknown').toLowerCase().replace(/[^a-z0-9]+/g, '-'),
    date,
    hour : time.split(':')[0],
    minsec : time.split(':').splice(1,2).join('-'),
    band : product.band,
    apid : metadata.apid,
    ms
  }

  if( ms && !productName.match(/^(mesoscale|conus|fulldisk|solar-imagery-euv-data)$/) ) {
    basePath = path.resolve('/', 
      productInfo.satellite,
      productInfo.product,
      productInfo.date,
      productInfo.hour,
      productInfo.minsec,
      ms,
      productInfo.apid
    );
  } else {
    basePath = path.resolve('/', 
      productInfo.satellite,
      productInfo.product,
      productInfo.date,
      productInfo.hour,
      productInfo.minsec,
      productInfo.apid
    );
  }

  logger.debug('Sending generic:  '+ basePath);

  await send(productInfo, path.join(basePath, 'metadata.json'), JSON.stringify(metadata));
  await send(productInfo, path.join(basePath, 'payload.bin'), payload);

  monitor.setMaxMetric(
    metric.type,
    'channel', 
    Date.now() - dateObj.getTime(),
    {
      apid: metadata.apid,
      channel: metadata.streamName
    }
  );
}

export default handleGenericMessage;