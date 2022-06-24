import {logger, config, KafkaConsumer, Monitoring} from '@ucd-lib/casita-worker';
import metrics from '../../init/google-cloud-metrics.js';
import handleImageMessage from './image.js';
import handleGenericMessage from './generic.js';

const METRIC_TYPE = 'custom.googleapis.com/grb/time-to-goes-product-writer';
let metric = metrics.find(item => item.type === METRIC_TYPE);

// init monitoring
let monitor = new Monitoring('goes-product-writer');
monitor.registerMetric(metric);
monitor.ensureMetrics();

// init kafka
let kafkaConsumer = KafkaConsumer({
  'group.id' : config.kafka.groups.productWriter
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

  console.log(metadata.type);
  if( metadata.type === 'image' ) {
    await handleImageMessage(metadata, payload, monitor, metric)
  } else {
    await handleGenericMessage(metadata, payload, monitor, metric);
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

  await kafkaConsumer.connect();
  logger.info(`Waiting for topic: ${config.kafka.topics.decoder}`);
  await kafkaConsumer.waitForTopics([config.kafka.topics.decoder]);

  logger.info(`Topic ready ${config.kafka.topics.decoder}, subscribing`);
  await kafkaConsumer.subscribe([config.kafka.topics.decoder]);

  kafkaConsumer.consume(async msg => {
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