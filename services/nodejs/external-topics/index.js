import {KafkaConsumer, logger, waitUntil, waitForTopics, config} from '@ucd-lib/casita-worker'
import { Server } from "socket.io";
import {getProperty} from 'dot-prop';
import topics from '../../init/kafka.js';

const io = new Server({ /* options */ });

let sockets = new Map();

let kafkaConsumer = KafkaConsumer({
  groupId : config.kafka.groups.external,
});

io.on("connection", (socket) => {
  map.set(socket.id, socket);

  logger.debug(`socket connected ${socket.id}`);

  socket.on("message", (msg) => {
    if( msg.cmd === 'setFilter' ) {
      if( !msg.key ) {
        socket.filter = null;
      } else {
        socket.filter = {key: msg.key, regex: new Regex(msg.regex)}
      }
    }
  });
});

io.on("disconnect", (socket) => {
  logger.debug(`socket disconnected ${socket.id}`);
  delete this.sockets[socket.id];
});

function onMessage(topic, msg) {
  msg = JSON.parse(msg.value.toString());

  logger.debug(`socket message: ${topic}`, msg);

  sockets.forEach((socket, id) => {
    if( !socket.filter ) {
      return socket.emit(topic, msg);
    }

    if( (getProperty(msg, socket.filter.key)+'').match(socker.filter.regex) ) {
      return socket.emit(topic, msg);
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