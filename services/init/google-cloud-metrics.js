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
  }
];
export default metrics;