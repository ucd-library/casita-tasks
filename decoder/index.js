const Processor = require('@ucd-lib/goes-r-packet-decoder/lib/binary-stream-processor');
const {logger} = require('@ucd-lib/krm-node-utils');
const kafkaSetup = require('./default-kafka-setup');

let processor = new Processor({
  name : process.env.GRB_FILE,
  consoleLogStatus : false,
  onStreamClosed : () => logger.warn(`${process.env.GRB_FILE} grb tail stream closed`),
  kafka : kafkaSetup
});

processor.pipe();