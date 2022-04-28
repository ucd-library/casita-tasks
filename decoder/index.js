const Processor = require('@ucd-lib/goes-r-packet-decoder/lib/binary-stream-processor');
const {logger, Monitor} = require('@ucd-lib/krm-node-utils');
const kafkaSetup = require('./default-kafka-setup');

let monitor = new Monitor(process.env.GRB_FILE);
let metric = {
  description: 'GRB product time to decoder',
  displayName: 'Time to decoder',
  type: 'custom.googleapis.com/grb/time_to_decoder',
  metricKind: 'GAUGE',
  valueType: 'INT64',
  unit: 'ms',
  labels: [
    {
      key: 'env',
      valueType: 'STRING',
      description: 'CASITA ENV',
    },
    {
      key: 'channel',
      valueType: 'STRING',
      description: 'UCD GRB Box Channel',
    }
  ]
};
monitor.registerMetric(metric);
monitor.ensureMetrics();

// 
// var dataObj = new Date(946728000000 + metadata.imagePayload.SECONDS_SINCE_EPOCH*1000);


let processor = new Processor({
  name : process.env.GRB_FILE,
  consoleLogStatus : false,
  onStreamClosed : () => logger.warn(`${process.env.GRB_FILE} grb tail stream closed`),
  preprocessCallback : msg => {
    msg.time = Date.now();
    var dataObj = new Date(946728000000 + msg.data.headers.SECONDS_SINCE_EPOCH*1000);

    monitor.setMaxMetric(
      metric,
      'channel', 
      Date.now() - dataObj.getTime(),
      {
        channel: process.env.GRB_FILE
      }
    );
  },
  kafka : kafkaSetup
});

processor.pipe();