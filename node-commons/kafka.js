const {Producer, utils} = require('@ucd-lib/node-kafka');
const config = require('./config');

class Kafka {
  constructor() {
    this.connected = false;
    this.producer = new Producer({
      'metadata.broker.list': config.kafka.host + ':' + config.kafka.port
    });
  }

  connect() {
    return producer.connect();
  }

  send(value, key) {
    // send message
    this.producer.produce({
      topic : config.kafka.topic,
      value, key
    })
  }

  flush() {
    return new Promise((resolve, reject) => {
      this.producer.flush(500, () => resolve());
    });
  }

}

module.export = new Kafka();