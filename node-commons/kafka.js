import {Kafka} from 'kafkajs';
import config from './config.js';
import {v4} from 'uuid';

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

  let existingTopics = (await admin.fetchTopicMetadata()).topics.map(item => item.topic);

  for( let topic of topics ) {
    if( !existingTopics.includes(topic) ) {
      return false;
    }
  }

  return true;
}

async function sendMessage(msg, kafkaProducer) {
  if( !kafkaProducer ) {
    kafkaProducer = KafkaProducer();
    await kafkaProducer.connect();
  }

  let response = await kafkaProducer.send({
    topic : msg.topic,
    messages : [{
      value : JSON.stringify({
        id : v4(),
        time : new Date().toISOString(),
        source : msg.source,
        datacontenttype : 'application/json',
        data : msg.data
      })
    }]
  });

  return {response, kafkaProducer};
}

export {KafkaConsumer, KafkaProducer, waitForTopics, sendMessage};