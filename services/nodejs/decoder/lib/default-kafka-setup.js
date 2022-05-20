import {logger, config} from '@ucd-lib/argonaut';

let khost = process.env.KAFKA_HOST || 'kafka';
let kport = process.env.KAFKA_PORT || '9092';
let kafkaHost = khost+':'+kport;

let clientOpts = {
  'metadata.broker.list' : config.kafka.host+':'+config.kafka.port,
  'message.max.bytes': 100000000+'', // must be a string
  'request.required.acks' : 1,
  'dr_cb': true, // delivery report
  'event_cb' : true
  // 'statistics.interval.ms' : 500 // for event.stats callback
}
if( process.env.KAFKA_CLIENT_DEBUG ) {
  clientOpts.debug = process.env.KAFKA_CLIENT_DEBUG;
}


const setup = {
  client : clientOpts,
  topic : {
    topic : process.env.DECODER_KAFKA_TOPIC || 'goesr-product',
    num_partitions: 10,
    replication_factor: 1,
    // TODO: this is set in decoder-krm-interface/index.js as well.  need to update both. badness
    config : {
      'retention.ms' : (1000 * 60 * 60)+'',
      // 'message.max.bytes': 25000000+''
      'max.message.bytes' : 100000000+''
    }
  },
  callbacks : {
    'ready' : () => logger.info(`${process.env.GRB_FILE} kafka producer ready`),
    'disconnected' : e => logger.warn(`${process.env.GRB_FILE} kafka producer disconnected`, e),
    'delivery-report' : (err, report) => {
        if( err ) logger.error(`${process.env.GRB_FILE} kafka delivery report error`, err, report);
    },
    'event' : e => logger.info(`${process.env.GRB_FILE} kafka event`, e),
    'event.log' : e => logger.error(`${process.env.GRB_FILE} kafka event.log`, e),
    'event.error' : e => logger.error(`${process.env.GRB_FILE} kafka event.error`, e),
    'event.stats' : e => logger.info('Kafka producer event.stats', e),
    'produce.error' : (e, msg) => {
        logger.error(`${process.env.GRB_FILE} local produce error`, e, msg.metadata)
    }
  }
}

export default setup