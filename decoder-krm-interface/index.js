const app = require('express')();
const http = require('http').createServer(app);
const Busboy = require('busboy');
const _fetch = require('node-fetch');
const FormData = require('form-data');
const path = require('path');
const cp = require('child_process');
const {apidUtils} = require('@ucd-lib/goes-r-packet-decoder');
const {logger, bus, Monitor, StartSubjectModel} = require('@ucd-lib/krm-node-utils');
const config = require('./config');
const kafka = bus.kafka;

let model = new StartSubjectModel({
  groupId: 'decoder-krm-interface'
});
// TODO: the decoder should be including this information
let SATELLITE = process.env.SATELLITE || 'west';

let monitor = new Monitor('decoder-krm-interface');
let metric = {
  description: 'GRB product time to krm interface',
  displayName: 'Time to KRM interface',
  type: 'custom.googleapis.com/grb/time_to_krm_interface',
  metricKind: 'GAUGE',
  valueType: 'INT64',
  unit: 'ms',
  labels: [
    {
      key: 'env',
      valueType: 'STRING',
      description: 'CASITA ENV',
    },
    {
      key: 'apid',
      valueType: 'STRING',
      description: 'GOES-R GRB APID',
    },
    {
      key: 'channel',
      valueType: 'STRING',
      description: 'UCD GRB Box Channel',
    }
  ]
};
monitor.registerMetric(metric);
monitor.ensureMetrics();

async function onMessage(msg) {
  logger.debug('Reading message of length: '+ msg.value.length);

  let length = msg.value.readUInt32BE(0);

  let metadata = JSON.parse(
    msg.value.slice(4, length+4).toString('utf-8')
  );
  let payload = null;

  if( msg.value.length > length + 4 ) {
    payload = msg.value.slice(length + 4, msg.value.length);
  }

  if( metadata.type === 'image' ) {
    await handleImageMessage(metadata, payload)
  } else {
    await handleGenericMessage(metadata, payload);
  }
}

async function handleGenericMessage(metadata, payload) {
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

  monitor.setMaxMetric(
    metric.type,
     'channel', 
     Date.now() - dataObj.getTime(),
     {
      apid: metadata.apid,
      channel: metadata.streamName
    }
  );

  logger.debug('Sending generic:  '+ basePath);

  await send(path.join(basePath, 'metadata.json'), JSON.stringify(metadata));
  await send(path.join(basePath, 'payload.bin'), payload);
}

async function handleImageMessage(metadata, payload) {
  let product = apidUtils.get(metadata.apid);
  if( !product.imageScale && !product.label ) return;

  var dataObj = new Date(946728000000 + metadata.imagePayload.SECONDS_SINCE_EPOCH*1000);
  var [date, time] = dataObj.toISOString().split('T');
  time = time.replace(/\..*/, '');

  let basePath = path.resolve('/', 
    SATELLITE,
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

  monitor.setMaxMetric(
    metric.type,
     'channel', 
     Date.now() - dataObj.getTime(),
     {
      apid: metadata.apid,
      channel: metadata.streamName
    }
  );

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
}

async function send(file, data) {
  try {
    await model.send(file, data);
  } catch(e) {
    logger.error('Decoder krm interface failed to send subject: '+file, e);
  }
}

(async function() {
  await model.connect();

  let kafkaConsumer = new kafka.Consumer({
    'group.id': config.decoder.groupId,
    'metadata.broker.list': config.decoder.kafka.host+':'+config.decoder.kafka.port,
    'enable.auto.commit': false,
    'auto.offset.reset' : 'earliest'
  });

  await kafka.utils.ensureTopic({
    topic: config.decoder.kafka.topic,
    num_partitions: 10,
    replication_factor: 1,
    // TODO: this is set in decoder/index.js as well.  need to update both. badness
    config : {
      'retention.ms' : (1000 * 60 * 60)+'',
      'max.message.bytes' : 100000000+''
    }
  }, {'metadata.broker.list': config.decoder.kafka.host+':'+config.decoder.kafka.port});

  await kafkaConsumer.connect();
  await kafkaConsumer.subscribe([config.decoder.kafka.topic]);

  try {
    await kafkaConsumer.consume(async msg => {
      try {
        await onMessage(msg);
      } catch(e) {
        logger.error('kafka message error', e);
      }
    });
  } catch(e) {
    logger.error('kafka consume error', e);
  }
})()
