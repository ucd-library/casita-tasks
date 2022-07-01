import {logger, config, KafkaConsumer, waitForTopics, waitUntil, Monitoring} from '@ucd-lib/casita-worker';
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
  groupId : config.kafka.groups.productWriter,
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
    await handleImageMessage(metadata, payload, monitor, metric)
  } else {
    await handleGenericMessage(metadata, payload, monitor, metric);
  }
}


(async function() {
  await kafkaConsumer.connect();

  logger.info(`Waiting for topic: ${config.kafka.topics.decoder}`);
  await waitForTopics([config.kafka.topics.decoder]);

  logger.info(`Topic ready ${config.kafka.topics.decoder}, subscribing`);
  await kafkaConsumer.subscribe({
    topics: [config.kafka.topics.decoder]
  });

  await kafkaConsumer.run({
    eachMessage: async ({topic, partition, message, heartbeat, pause}) => {
      try {
        await onMessage(message);
      } catch(e) {
        logger.error('kafka message error', e);
      }
    }
  });

})()