const metrics = [
  {
    description: 'Decorder to GOES product writer service time',
    displayName: 'Time to GOES product writer service',
    type: 'custom.googleapis.com/grb/time-to-goes-product-writer',
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
    description: 'Decorder to GOES product writer service time',
    displayName: 'Time to GOES product writer service',
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
    description: 'Decorder to GOES product writer service time',
    displayName: 'Time to GOES product writer service',
    type: 'custom.googleapis.com/grb/worker-exec-time',
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
    description: 'CaSITA worker task execution details (time)',
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
    description: 'CaSITA worker task execution details (status)',
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