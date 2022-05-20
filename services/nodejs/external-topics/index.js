const path = require('path');
const fs = require('fs');
const Worker = require('/service/lib/worker');
const { config, logger, bus } = require('@ucd-lib/krm-node-utils');
const kafka = bus.kafka;

class ExternalTopicsWorker extends Worker {

  constructor() {
    super();

    this.topic = 'lightning'

    this.kafkaPubTopicProducer = new kafka.Producer({
      'metadata.broker.list': config.kafka.host+':'+config.kafka.port
    });
  }

  async connect() {
    await kafka.utils.ensureTopic({
      topic: this.topic,
      num_partitions: 10,
      replication_factor: 1,
      config : {
        'retention.ms' : (1000 * 60 * 60 * 24 * 4)+'',
        'max.message.bytes' : 100000000+''
      }
    }, {'metadata.broker.list': config.kafka.host+':'+config.kafka.port});
    await super.connect();

    await this.kafkaPubTopicProducer.connect();
  }

  async exec(msg) {
    let file = path.join(config.fs.nfsRoot, msg.data.ready[0].replace('file:///', ''));

    try {
      await this.sendlightning(msg.data.args, file);
    } catch (e) {
      logger.error(e);
    }
  }

  async sendlightning(properties, file) {
    if (!fs.existsSync(file)) {
      logger.error('File does not exist: ' + file);
      return;
    }

    let data = JSON.parse(fs.readFileSync(file, 'utf-8'));

    this.kafkaPubTopicProducer.produce({
      topic : this.topic,
      value: {properties, data}
    });
  }

}

let worker = new ExternalTopicsWorker();
worker.connect();