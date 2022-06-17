import {apidUtils} from '@ucd-lib/goes-r-packet-decoder';
import send from './send.js';

async function handleGenericMessage(metadata, payload, monitor) {
  var dataObj = new Date(946728000000 + metadata.headers.SECONDS_SINCE_EPOCH*1000);
  var [date, time] = dataObj.toISOString().split('T');
  time = time.replace(/\..*/, '');

  let product = apidUtils.get(metadata.apid);
  let ms = false;
  if( metadata.spacePacketHeaders && metadata.spacePacketHeaders && 
    metadata.spacePacketHeaders.secondary ) {
    ms = metadata.spacePacketHeaders.secondary.MILLISECONDS_OF_THE_DAY+'';
  }
  let basePath;

  let productName = (product.imageScale || product.label || 'unknown').toLowerCase().replace(/[^a-z0-9]+/g, '-');

  if( ms && !productName.match(/^(mesoscale|conus|fulldisk|solar-imagery-euv-data)$/) ) {
    basePath = path.resolve('/', 
      SATELLITE,
      (product.imageScale || product.label || 'unknown').toLowerCase().replace(/[^a-z0-9]+/g, '-'),
      date,
      time.split(':')[0],
      time.split(':').splice(1,2).join('-'),
      ms,
      metadata.apid
    );
  } else {
    basePath = path.resolve('/', 
      SATELLITE,
      (product.imageScale || product.label || 'unknown').toLowerCase().replace(/[^a-z0-9]+/g, '-'),
      date,
      time.split(':')[0],
      time.split(':').splice(1,2).join('-'),
      metadata.apid
    );
  }

  logger.debug('Sending generic:  '+ basePath);

  await send(path.join(basePath, 'metadata.json'), JSON.stringify(metadata));
  await send(path.join(basePath, 'payload.bin'), payload);

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

export default handleGenericMessage;