import {logger, config, KafkaConsumer, Monitor} from '@ucd-lib/casita-worker';
import metric from './metric.js';
import handleImageMessage from './image.js';
import handleGenericMessage from './generic.js';

// init monitoring
let monitor = new Monitor('goes-product-writer');
monitor.registerMetric(metric);
monitor.ensureMetrics();

// init kafka
let kafkaConsumer = new KafkaConsumer({
  groupId : config.kafka.groups.productWriter
});

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
    await handleImageMessage(metadata, payload, monitor)
  } else {
    await handleGenericMessage(metadata, payload, monitor);
  }
}


(async function() {
  // await model.connect();

  // TODO: move to kafka init
  // await kafka.utils.ensureTopic({
  //   topic: config.decoder.kafka.topic,
  //   num_partitions: 10,
  //   replication_factor: 1,
  //   // TODO: this is set in decoder/index.js as well.  need to update both. badness
  //   config : {
  //     'retention.ms' : (1000 * 60 * 60)+'',
  //     'max.message.bytes' : 100000000+''
  //   }
  // }, {'metadata.broker.list': config.decoder.kafka.host+':'+config.decoder.kafka.port});

  await kafkaConsumer.client.connect();
  await kafkaConsumer.client.subscribe([config.kafka.topics.decoder]);

  await kafkaProducer.client.connect();

  kafkaConsumer.client.consume(async msg => {
    try {
      await onMessage(msg);
      await sleep(25);
    } catch(e) {
      logger.error('kafka message error', e);
    }
  });
})()

async function sleep(time) {
  return new Promise((resolve, reject) => {
    setTimeout(() => resolve(), time);
  });
}