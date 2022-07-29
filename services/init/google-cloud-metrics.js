const metrics = [
  // decoder
  {
    description: 'GRB product capture time to fully decoded',
    displayName: 'Time to decoded',
    type: 'custom.googleapis.com/casita/time_to_decoded',
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
  },
  {
    description: 'GRB Decoder megabits per second',
    displayName: 'GRB Decoder data rate',
    type: 'custom.googleapis.com/casita/decoder_data_rate',
    metricKind: 'GAUGE',
    valueType: 'INT64',
    unit: 'Mbit',
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
  },
  {
    description: 'GRB Decoder packets per second',
    displayName: 'GRB Decoder packet rate',
    type: 'custom.googleapis.com/casita/decoder_packet_rate',
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
      },
      {
        key: 'valid',
        valueType: 'BOOL',
        description: 'Valid or invalid packet',
      }
    ]
  },


  // Product writer
  {
    description: 'Decorder to GOES product writer service time',
    displayName: 'Time to GOES product writer service',
    type: 'custom.googleapis.com/casita/time-to-disk',
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
        key: 'apid',
        valueType: 'STRING',
        description: 'GOES-R GRB APID',
      },
      {
        key: 'channel',
        valueType: 'STRING',
        description: 'UCD GRB Box Channel',
      }
    ]
  },

  // argonaut
  {
    description: 'Time to for argonaut to group and send message',
    displayName: 'Argonaut time',
    type: 'custom.googleapis.com/casita/a6t-compose-time',
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
        key: 'task',
        valueType: 'STRING',
        description: 'CASITA Task',
      }
    ]
  },


  // worker
  {
    description: 'Time spent in tasks kafka topic.  a6t -> worker',
    displayName: 'Time to Worker',
    type: 'custom.googleapis.com/grb/time-to-worker',
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
        key: 'apid',
        valueType: 'STRING',
        description: 'GOES-R GRB APID',
      },
      {
        key: 'channel',
        valueType: 'STRING',
        description: 'UCD GRB Box Channel',
      }
    ]
  },
  {
    description: 'CaSITA worker task execution time',
    displayName: 'CaSITA - Worker Execution Time',
    type: 'custom.googleapis.com/casita/worker-execution-time',
    metricKind: 'GAUGE',
    valueType: 'INT64',
    unit: 'ms',
    labels: [
      {
        key: 'instance',
        valueType: 'STRING',
        description: 'CaSITA instance name',
      },
      {
        key: 'command',
        valueType: 'STRING',
        description: 'bash command that was run',
      },
      {
        key: 'status',
        valueType: 'STRING',
        description: 'ex: success, error',
      }
    ]
  },
  {
    description: 'CaSITA worker task execution status',
    displayName: 'CaSITA - Worker Execution Status',
    type: 'custom.googleapis.com/casita/worker-execution-status',
    metricKind: 'GAUGE',
    valueType: 'INT64',
    unit: '1',
    labels: [
      {
        key: 'instance',
        valueType: 'STRING',
        description: 'CaSITA instance name',
      },
      {
        key: 'command',
        valueType: 'STRING',
        description: 'bash command that was run',
      },
      {
        key: 'status',
        valueType: 'STRING',
        description: 'ex: success, error',
      }
    ]
  }
];
export default metrics;