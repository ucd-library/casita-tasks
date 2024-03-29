import amqp from 'amqplib';
import logger from './logger.js';
import config from './config.js';
import waitUntil from './wait-until.js';

class RabbitMQ {

  constructor() {
    this.host = `${config.rabbitMq.host}:${config.rabbitMq.port}`;
  }

  async connect(opts={}) {
    if( !this.connectPromise ) {
      this.connectPromise = new Promise(async (resolve, reject) => {
        this.connectResolve = resolve;
      });
    }

    this.opts = Object.assign({
      prefetchCount : 1
    }, opts);
    this.onMessageCallback = opts.onMessageCallback;

    logger.info(`attempting connection to RabbitMQ server: amqp://${this.host}`);
    
    try {
      await waitUntil(config.rabbitMq.host, config.rabbitMq.port);

      this.conn = await amqp.connect(`amqp://${this.host}`, config.rabbitMq.config);
      this.conn.on('close', () => this.onConnectionClosed());
      this.conn.on('error', e => this.onConnectionClosed(e));
      
      this.channel = await this.conn.createChannel();
      logger.info(`connected to RabbitMQ server, setting prefetch to ${this.opts.prefetchCount}`);

      let resolve = this.connectResolve;
      this.connectPromise = null;
      this.connectResolve = null;
      resolve();

    } catch(e) {
      logger.warn(`Error attempting to connect to RabbitMQ, will try again`, e);
      setTimeout(() => this.connect(), 2000);
    }

    return this.connectPromise;
  }

  ack(msg) {
    return this.channel.ack(msg);
  }

  nack(msg) {
    return this.channel.nack(msg);
  }

  listen(queue, callback) {
    this.channel.prefetch(this.opts.prefetchCount);
    this.channel.consume(queue, msg => callback(msg));
  }

  async createQueues(queues=[]) {
    if( !Array.isArray(queues) ) queues = [queues];
    for( let queue of queues ) {
      logger.info(`ensuring queue ${queue}`);
      await this.channel.assertQueue(queue, {durable: true});
    }
  }

  async deleteQueues(queues=[]) {
    if( !Array.isArray(queues) ) queues = [queues];
    for( let queue of queues ) {
      logger.info(`removing queue ${queue}`);
      await this.channel.deleteQueue(queue);
    }
  }

  async onConnectionClosed(e) {
    logger.info(`RabbitMQ connection closed!, will attempt reconnect`, e);
    setTimeout(() => this.connect(), 1000);
  }

  // async onMessage(msg) {
  //   logger.debug(`rabbitmq recieved message`);
  //   let data = JSON.parse(msg.content.toString());
  //   if( this.onMessageCallback ) {
  //     await this.onMessageCallback(msg, data);
  //   } else {
  //     logger.warn(`rabbitmq recieved message but no callback handler set`);
  //   }
  // }

  send(queue, msg, priority) {
    if( priority === undefined ) {
      priority = config.rabbitMq.defaultPriority;
      if( typeof msg === 'object' && msg.data && msg.data.priority !== undefined ) {
        priority = msg.data.priority;
      }
    }

    if( typeof msg === 'object' ) msg = JSON.stringify(msg);
    msg = Buffer.from(msg);
    return this.channel.sendToQueue(queue, msg, {priority});
  }
}

export default RabbitMQ;