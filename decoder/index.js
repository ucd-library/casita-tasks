const Processor = require('@ucd-lib/goes-r-packet-decoder/lib/binary-stream-processor');
const apidUtils = require('@ucd-lib/goes-r-packet-decoder/lib/utils/apid');
const {logger, Monitor} = require('@ucd-lib/krm-node-utils');
const kafkaSetup = require('./default-kafka-setup');

let monitor = new Monitor(process.env.GRB_FILE);
let ttdMetric = {
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
    },
    {
      key: 'apid',
      valueType: 'STRING',
      description: 'APID',
    }
  ]
};
let drMetric = {
  description: 'GRB Decoder megabits per second',
  displayName: 'GRB Decoder data rate',
  type: 'custom.googleapis.com/grb/decoder_data_rate',
  metricKind: 'GAUGE',
  valueType: 'INT64',
  unit: 'MBit',
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
let prMetric = {
  description: 'GRB Decoder packets per second',
  displayName: 'GRB Decoder packet rate',
  type: 'custom.googleapis.com/grb/decoder_packet_rate',
  metricKind: 'GAUGE',
  valueType: 'INT64',
  unit: '1',
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
let priMetric = {
  description: 'GRB Decoder invalid packets per second',
  displayName: 'GRB Decoder invalid packet rate',
  type: 'custom.googleapis.com/grb/decoder_invalid_packet_rate',
  metricKind: 'GAUGE',
  valueType: 'INT64',
  unit: '1',
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
monitor.registerMetric(ttdMetric);
monitor.registerMetric(drMetric);
monitor.registerMetric(prMetric);
monitor.registerMetric(priMetric);
monitor.ensureMetrics();

let processor = new Processor({
  name : process.env.GRB_FILE,
  consoleLogStatus : false,
  statusCallback : msg => {
    let {packetsPerSecond, mbytesPerSecond, invalidPacketsPerSecond, uptime} = msg;

    monitor.incrementMetric(
      drMetric.type,
      'channel', 
      {
        channel: process.env.GRB_FILE,
      },
      mbytesPerSecond
    );

    monitor.incrementMetric(
      prMetric.type,
      'channel', 
      {
        channel: process.env.GRB_FILE
      },
      packetsPerSecond
    );

    monitor.incrementMetric(
      priMetric.type,
      'channel', 
      {
        channel: process.env.GRB_FILE
      },
      invalidPacketsPerSecond
    );

  },
  onStreamClosed : () => logger.warn(`${process.env.GRB_FILE} grb tail stream closed`),
  preprocessCallback : msg => {
    msg.time = Date.now();
    var dataObj = new Date(946728000000 + msg.data.headers.SECONDS_SINCE_EPOCH*1000);

    let apid = msg.data.spHeaders.primary.APPLICATION_PROCESS_IDENTIFIER;
    if( Date.now() - dataObj.getTime() < 0 ) return;

    monitor.setMaxMetric(
      ttdMetric.type,
      'apid', 
      Date.now() - dataObj.getTime(),
      {
        channel: process.env.GRB_FILE,
        apid : apid.toString(16)
      }
    );
  },
  kafka : kafkaSetup
});

processor.pipe();