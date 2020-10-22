const Processor = require('@ucd-lib/goes-r-packet-decoder/lib/binary-stream-processor');

let serverUrl = 'kafka:9092';
if( process.env.DECODER_KAFKA_HOST && process.env.DECODER_KAFKA_PORT ) {
  serverUrl = process.env.DECODER_KAFKA_HOST + ':' + process.env.DECODER_KAFKA_PORT;
}

let processor = new Processor({
  name : process.env.GRB_FILE,
  consoleLogStatus : false,
  kafka : {
    server : {
      'metadata.broker.list' : serverUrl
      // 'message.max.bytes': 100000000+''
    },
    topic : {
      topic : process.env.DECODER_KAFKA_TOPIC || 'goes-r-stream',
      num_partitions: 10,
      replication_factor: 1,
      // TODO: this is set in decoder-krm-interface/index.js as well.  need to update both. badness
      config : {
        'retention.ms' : (1000 * 60 * 60)+'',
        'max.message.bytes' : 100000000+''
      }
    }
  }
});

processor.pipe();