import kafka from '@ucd-lib/node-kafka';
import config from './config.js';

const {Producer, Consumer, utils} = kafka;

class KafkaProducer {
  constructor(kconfig, pollInterval=100) {

    if( !kconfig['metadata.broker.list'] ) {
      kconfig['metadata.broker.list'] = config.kafka.host + ':' + config.kafka.port;
    }

    this.client = new Producer(kconfig);

    this.client.setPollInterval(100);
  }

}

class KafkaConsumer {
  constructor(kconfig={}, topicConfig={}) {
    this.connected = false;

    if( !kconfig['metadata.broker.list'] ) {
      kconfig['metadata.broker.list'] = config.kafka.host + ':' + config.kafka.port;
    }
    if( !topicConfig['auto.offset.reset'] ) {
      config['auto.offset.reset'] = 'earliest';
    }

    this.client = new Consumer(kconfig, topicConfig);
  }
}

export {KafkaConsumer, KafkaProducer};