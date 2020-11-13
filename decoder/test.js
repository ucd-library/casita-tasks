const path = require('path');
const fs = require('fs');
const {logger} = require('@ucd-lib/krm-node-utils');
const Processor = require('@ucd-lib/goes-r-packet-decoder/lib/binary-stream-processor');
const kafkaSetup = require('./default-kafka-setup');

let processor = new Processor({
  live: true,
  name : process.env.GRB_FILE,
  consoleLogStatus : false,
  filter : /^b1$/i,
  onStreamClosed : () => logger.warn(`${process.env.GRB_FILE} grb tail stream closed`),
  kafka : kafkaSetup
});

processor.pipe();

// (async function() {
//   await processor.kafkaConnecting;
//   processor.pipe(fs.createReadStream(path.join(__dirname, 'testsecdecorded.dat')));
//   // setTimeout(() => process.exit(), 4000);
// })()

