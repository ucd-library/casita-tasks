import decoder from '@ucd-lib/goes-r-packet-decoder';
import {logger} from '@ucd-lib/argonaut';
import kafkaSetup from './lib/default-kafka-setup.js';

const {BinaryStreamProcessor} = decoder;

let processor = new BinaryStreamProcessor({
  name : process.env.GRB_FILE,
  consoleLogStatus : true,
  onStreamClosed : () => logger.warn(`${process.env.GRB_FILE} grb tail stream closed`),
  kafka : kafkaSetup
});

processor.pipe();