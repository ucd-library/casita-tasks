import decoder from '@ucd-lib/goes-r-packet-decoder';
import {logger, Monitoring} from '@ucd-lib/casita-worker';
import metrics from '../../init/google-cloud-metrics.js';
import kafkaSetup from './lib/default-kafka-setup.js';

const {BinaryStreamProcessor} = decoder;

const DECODED_METRIC_TYPE = 'custom.googleapis.com/casita/time_to_decoded';
const DATA_RATE_METRIC_TYPE = 'custom.googleapis.com/casita/decoder_data_rate';
const PACKET_RATE_METRIC_TYPE = 'custom.googleapis.com/casita/decoder_packet_rate';


// init monitoring
let monitor = new Monitoring('decoder');
monitor.registerMetric(metrics.find(item => item.type === DECODED_METRIC_TYPE));
monitor.registerMetric(metrics.find(item => item.type === DATA_RATE_METRIC_TYPE), {average: true});
monitor.registerMetric(metrics.find(item => item.type === PACKET_RATE_METRIC_TYPE), {average: true});


let processor = new BinaryStreamProcessor({
  name : process.env.GRB_FILE,
  H_SPACECRAFT_ID : process.env.H_SPACECRAFT_ID || 228,
  consoleLogStatus : (process.env.LOG_STATUS === 'true'),
  onStreamClosed : () => logger.warn(`${process.env.GRB_FILE} grb tail stream closed`),
  kafka : kafkaSetup,
  statusCallback : msg => {
    let {packetsPerSecond, mbitsPerSecond, invalidPacketsPerSecond, uptime} = msg;

    monitor.incrementMetric(
      DATA_RATE_METRIC_TYPE,
      'channel', 
      {
        channel: process.env.GRB_FILE,
      },
      mbitsPerSecond
    );

    monitor.incrementMetric(
      PACKET_RATE_METRIC_TYPE,
      'status', 
      {
        channel: process.env.GRB_FILE,
        status : 'valid'
      },
      packetsPerSecond
    );

    monitor.incrementMetric(
      PACKET_RATE_METRIC_TYPE,
      'status', 
      {
        channel: process.env.GRB_FILE,
        status: 'invalid'
      },
      invalidPacketsPerSecond
    );
  },
  preprocessCallback : msg => {
    msg.time = Date.now();
    var dataObj = new Date(946728000000 + msg.data.headers.SECONDS_SINCE_EPOCH*1000);

    let apid = msg.data.spHeaders.primary.APPLICATION_PROCESS_IDENTIFIER;
    if( Date.now() - dataObj.getTime() < 0 ) return;

    monitor.setMaxMetric(
      DECODED_METRIC_TYPE,
      'apid', 
      Date.now() - dataObj.getTime(),
      {
        channel: process.env.GRB_FILE,
        apid : apid.toString(16)
      }
    );
  }
});

processor.pipe();