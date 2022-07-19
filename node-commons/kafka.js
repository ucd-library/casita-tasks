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
  while( !(await _waitForTopics(topics)) ) {
    await sleep();
  }
}

async function _waitForTopics(topics) {
  if( !admin ) {
    admin = kafka.admin();
    await admin.connect();
  }

  let existingTopics = (await admin.fetchTopicMetadata()).topics.map(item => item.name);

  for( let topic of topics ) {
    if( !existingTopics.includes(topic) ) {
      return false;
    }
  }

  return true;
}

async function sleep(time) {
  return new Promise((resolve, reject) => {
    setTimeout(() => resolve(), time || 500);
  });
}

async function sendMessage(msg, kafkaProducer) {
  if( !kafkaProducer ) {
    kafkaProducer = KafkaProducer();
    await kafkaProducer.connect();
  }

  let messages = [{
    value : JSON.stringify({
      id : v4(),
      time : new Date().toISOString(),
      source : msg.source,
      datacontenttype : 'application/json',
      data : msg.data
    })
  }];

  let response = await kafkaProducer.send({
    topic : msg.topic,
    messages
  });

  let extResponse = null;
  if( msg.external ) {
    extResponse = await kafkaProducer.send({
      topic : msg.topic+'-ext',
      messages
    });
  }

  return {response, extResponse, kafkaProducer};
}

export {KafkaConsumer, KafkaProducer, waitForTopics, sendMessage};