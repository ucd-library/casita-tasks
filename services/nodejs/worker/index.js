import {logger, config, KafkaConsumer, waitForTopics, waitUntil, Monitoring} from '@ucd-lib/casita-worker';
import metrics from '../../init/google-cloud-metrics.js';
import exec from './exec.js';

const METRIC_TYPE = 'custom.googleapis.com/grb/time-to-worker';
let metric = metrics.find(item => item.type === METRIC_TYPE);
let monitor = new Monitoring('casita-worker');
monitor.registerMetric(metric);

// init kafka
let kafkaConsumer = KafkaConsumer({
  groupId : config.kafka.groups.worker,
});

(async function() {
  await waitUntil(config.kafka.host, config.kafka.port);
  await kafkaConsumer.connect();

  logger.info(`Waiting for topic: ${config.kafka.topics.tasks}`);
  await waitForTopics([config.kafka.topics.tasks]);

  logger.info(`Topic ready ${config.kafka.topics.tasks}, subscribing`);
  await kafkaConsumer.subscribe({
    topics: [config.kafka.topics.tasks]
  });

  let lastMessageCompletedAt = Date.now();

  await kafkaConsumer.run({
    heartbeatInterval	: 10 * 1000,
    eachMessage: async ({topic, partition, message, heartbeat, pause}) => {
      try {
        message.value = JSON.parse(message.value.toString())
        logger.debug('casita worker received message: ', {topic, partition, offset: message.offset});
        logger.debug('kafka message fetch time: ', (Date.now()-lastMessageCompletedAt));

        let timestamp = new Date(message.value.time).getTime();
        monitor.setMaxMetric(
          METRIC_TYPE,
          'task',
          Date.now() - timestamp,
          {
            task: message.value.data.task
          }
        );

        await exec(message.value.data.cmd);
      } catch(e) {
        logger.error('kafka message error', e);
      }
      lastMessageCompletedAt = Date.now();
    }
  });

})()