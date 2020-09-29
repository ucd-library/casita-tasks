const Processor = require('@ucd-lib/goes-r-packet-decoder/lib/binary-stream-processor');

let serverUrl = 'kafka:9200';
if( process.env.DECODER_KAFKA_HOST && process.env.DECODER_KAFKA_PORT ) {
  serverUrl = process.env.DECODER_KAFKA_HOST + ':' + process.env.DECODER_KAFKA_PORT;
}

let processor = new Processor({
  name : process.env.GRB_FILE,
  consoleLogStatus : false,
  kafka : {
    server : {
      'metadata.broker.list' : serverUrl
    },
    topic : {
      topic : process.env.DECODER_KAFKA_TOPIC || 'goes-r-stream',
      num_partitions: 10,
      options : {
        'retention.ms' : 1000 * 60 * 15
      }
    }
  }
});

processor.pipe();