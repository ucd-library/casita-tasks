import kafka from '@ucd-lib/node-kafka';
import config from './config.js';

const {Producer, Consumer, utils} = kafka;

function KafkaProducer(kconfig={}, pollInterval=100) {
  if( !kconfig['metadata.broker.list'] ) {
    kconfig['metadata.broker.list'] = config.kafka.host + ':' + config.kafka.port;
  }

  let producer = new Producer(kconfig);

  producer.client.setPollInterval(pollInterval);
  return producer;
}

function KafkaConsumer(kconfig={}, topicConfig={}) {
  if( !kconfig['metadata.broker.list'] ) {
    kconfig['metadata.broker.list'] = config.kafka.host + ':' + config.kafka.port;
  }
  if( !topicConfig['auto.offset.reset'] ) {
    topicConfig['auto.offset.reset'] = 'earliest';
  }

  return new Consumer(kconfig, topicConfig);
}

export {KafkaConsumer, KafkaProducer};