import {KafkaConsumer, logger, waitUntil, waitForTopics, config} from '@ucd-lib/casita-worker'
import { Server } from "socket.io";
import {getProperty} from 'dot-prop';
import topics from '../../init/kafka.js';

const io = new Server({
  cors: {
    origin: "*",
    credentials: false
  },
  path : '/_/ws/'
 });

let sockets = new Map();

let kafkaConsumer = KafkaConsumer({
  groupId : config.kafka.groups.external,
});

io.on('connection', (socket) => {
  sockets.set(socket.id, socket);

  logger.debug(`socket connected ${socket.id}`);

  socket.on('listen', (msg) => {
    logger.debug(`setting filters for socket ${socket.id}`, msg.filters);
    if( !msg.filters ) {
      socket.filters = null;
    } else {
      socket.filters = msg.filters.map(item => ({
        key: item.key, 
        regex: new RegExp(item.regex),
        topic: item.topic
      }))
    }
  });
});

io.on("disconnect", (socket) => {
  logger.debug(`socket disconnected ${socket.id}`);
  sockets.delete(socket.id);
});

function onMessage(topic, msg) {
  msg = JSON.parse(msg.value.toString());

  logger.debug(`socket message: ${topic}`, msg);
  topic = topic.replace(/-ext$/, '');

  sockets.forEach((socket, id) => {
    if( !socket.filters ) return;

    for( let filter of socket.filters ) {
      // check filter is for topic
      if( filter.topic !== topic ) continue;

      // just listening to all messages on topic
      if( !filter.regex || !filter.key ) {
        return socket.emit('message', {topic, message: msg});
      }

      // check key and regex for filtered message match
      if( (getProperty(msg, filter.key)+'').match(filter.regex) ) {
        return socket.emit('message', {topic, message: msg});
      }
    }
  });
}

(async function() {
  waitUntil(config.kafka.host, config.kafka.port);
  await kafkaConsumer.connect();

  let externalTopics = topics.map(item => item.name)
    .filter(name => name.match(/-ext$/));

  logger.info(`Waiting for external topic: ${externalTopics.join(', ')}`);
  await waitForTopics(externalTopics);

  logger.info(`External topics ready ${externalTopics.join(', ')}, subscribing`);
  
  await kafkaConsumer.subscribe({
    topics: externalTopics
  });

  await kafkaConsumer.run({
    eachMessage: async ({topic, partition, message, heartbeat, pause}) => {
      try {
        await onMessage(topic, message);
      } catch(e) {
        logger.error('kafka message error', e);
      }
    }
  });

})()



io.listen(3000);