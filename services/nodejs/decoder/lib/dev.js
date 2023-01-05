import decoder from '@ucd-lib/goes-r-packet-decoder';
import {logger, Monitoring} from '@ucd-lib/casita-worker';
import kafkaSetup from './default-kafka-setup.js';

const {BinaryStreamProcessor} = decoder;


let processor = new BinaryStreamProcessor({
  name : process.env.GRB_FILE,
  H_SPACECRAFT_ID : process.env.H_SPACECRAFT_ID || 228,
  filter : /^b6$/i,
  consoleLogStatus : (process.env.LOG_STATUS === 'true'),
  onStreamClosed : () => logger.warn(`${process.env.GRB_FILE} grb tail stream closed`),
  kafka : kafkaSetup
});

processor.pipe();