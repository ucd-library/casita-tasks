const {config, logger} = require('@ucd-lib/krm-node-utils');
const monitoring = require('@google-cloud/monitoring');

class Monitoring {

  constructor() {
    let clientConfig = {};
    if( config.google.serviceAccountFile ) {
      clientConfig.keyFilename = config.google.serviceAccountFile;
    }
    this.client = new monitoring.MetricServiceClient(clientConfig);

    this.maxTTD = {};

    this.TYPES = {
      TTD : 'custom.googleapis.com/grb/time_to_disk'
    }

    this.ensureTypes();

    setInterval(() => this.write(), 1000 * 30);
  }

  async ensureTypes() {
    const ttd = {
      name: this.client.projectPath(config.google.projectId),
      metricDescriptor: {
        description: 'GRB product time to disk',
        displayName: 'Time to Disk',
        type: this.TYPES.TTD,
        metricKind: 'GAUGE',
        valueType: 'INT64',
        unit: '{ms}',
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
    };

    await this.client.createMetricDescriptor(ttd);
  }

  addTTD(apid, ttd, time, channel) {
    if( !this.maxTTD[apid] ) {
      this.maxTTD[apid] = {ttd, time, channel};
      return;
    }
    if( this.maxTTD[apid].ttd > ttd ) return;
    this.maxTTD[apid] = {ttd, time, channel};
  }

  async write() {
    // write ttd metrics
    for( let apid in this.maxTTD ) {
      let value = this.maxTTD[apid];
      if( value === null || value === undefined ) continue;

      let dataPoint = {
        interval: {
          endTime: {
            seconds: value.time.getTime() / 1000,
          },
        },
        value: {
          int64Value: value.ttd+'',
        },
      };
      this.maxTTD[apid] = null;
    
      let timeSeriesData = {
        metric: {
          type: this.TYPES.TTD,
          labels: {
            apid, 
            channel : value.channel,
            env : config.env || 'not-set'
          },
        },
        resource: {
          type: 'global',
          labels: {
            project_id: config.google.projectId,
          },
        },
        points: [dataPoint],
      };
    
      let request = {
        name: this.client.projectPath(config.google.projectId),
        timeSeries: [timeSeriesData],
      };
    
      // Writes time series data
      try {
        let result = await this.client.createTimeSeries(request);
      } catch(e) {
        logger.warn('error writing ttd metric', e);
      }
    }
  }

}

module.exports = new Monitoring();