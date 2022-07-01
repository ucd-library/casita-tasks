import {Kafka} from 'kafkajs';
import config from './config.js';

const kafka = new Kafka({
  clientId: config.kafka.clientId,
  brokers: [config.kafka.host + ':' + config.kafka.port]
});
let admin;

function KafkaProducer(kconfig={}, pollInterval=100) {
  return kafka.producer();
}

function KafkaConsumer(kconfig={}) {
  return kafka.consumer(kconfig)
}

async function waitForTopics(topics) {
  if( !admin ) {
    admin = kafka.admin();
    await admin.connect();
  }
  console.log(await admin.fetchTopicMetadata())
  let existingTopics = (await admin.fetchTopicMetadata()).topics.map(item => item.topic);

  for( let topic of topics ) {
    if( !existingTopics.includes(topic) ) {
      return false;
    }
  }

  return true;
}

export {KafkaConsumer, KafkaProducer, waitForTopics};