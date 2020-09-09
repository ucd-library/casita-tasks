const app = require('express')();
const http = require('http').createServer(app);
const {logger, config, bus, utils} = require('@ucd-lib/krm-node-utils');
const io = require('socket.io')(http, {
  path : '/_/ws'
});

const registrations = {};

const kafka = bus.kafka;
const KAFKA_GROUP_ID = 'ws-service';

io.on('connection', (socket) => {
  logger.info('a user connected!', socket.id);
  socket.on('chat', (msg) => {
    console.log('message: ' + msg);
  });

  socket.on('listen', (msg) => {
    msg = JSON.parse(msg);
    let subjects = msg.subjects || [];

    let current = registrations[socket.id];
    if( !current ) {
      current = {
        subjects : [],
        socket : socket
      }
      registrations[socket.id] = current;
    }

    subjects.forEach(subject => {
      let exists = current.subjects.find(ele => ele.href === subject);
      if( exists ) return;
      current.subjects.push(utils.subjectParser(subject));
    });
  });

  socket.on('unlisten', (msg) => {
    msg = JSON.parse(msg);
    let subjects = msg.subjects || [];

    let current = registrations[socket.id];
    if( !current ) {
      current = {
        subjects : [],
        socket : socket
      }
      registrations[socket.id] = current;
    }

    subjects.forEach(subject => {
      let index = current.subjects.findIndex(ele => ele.href === subject);
      if( index === -1 ) return;
      current.subjects.splice(index, 1);
    });
  });

  socket.on('disconnect', () => {
    if( registrations[socket.id] ) {
      delete registrations[socket.id];
    }
  });
});

http.listen(3000, () => {
  logger.info('listening on *:3000');
});

(async function() {
  this.kafkaConsumer = new kafka.Consumer({
    'group.id': KAFKA_GROUP_ID,
    'metadata.broker.list': config.kafka.host+':'+config.kafka.port,
    'enable.auto.commit': true
  });
  await this.kafkaConsumer.connect();

  await this.kafkaConsumer.assign([{
    topic : config.kafka.topics.subjectReady,
    partition : 0
  }]);

  await this.kafkaConsumer.consume(msg => {
    let raw = msg.value.toString('utf-8');
    msg = JSON.parse(raw);

    let id, reg, subject;
    for( id in registrations ) {
      reg = registrations[id];
      for( subject of reg.subjects ) {
        if( subject.path.regex.test(msg.subject) ) {
          reg.socket.emit('message', raw);
          break;
        }
      }
    }
  });
})();