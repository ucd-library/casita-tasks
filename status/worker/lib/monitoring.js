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
    this.maxCPGT = {};


    this.TYPES = {
      TTD : 'custom.googleapis.com/grb/time_to_disk',
      CPGT : 'custom.googleapis.com/krm/comp_png_gen_time'
    }

    // this.ensureTypes();

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

    const cpgt = {
      name: this.client.projectPath(config.google.projectId),
      metricDescriptor: {
        description: 'GRB jp2 composite and png conversion time',
        displayName: 'PNG Conversion',
        type: this.TYPES.CPGT,
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
            key: 'block',
            valueType: 'STRING',
            description: 'Product top/left values',
          }
        ]
      },
    };

    await this.client.createMetricDescriptor(cpgt);
  }

  addTTD(apid, ttd, time, channel) {
    if( !this.maxTTD[apid] ) {
      this.maxTTD[apid] = {ttd, time, channel};
      return;
    }
    if( this.maxTTD[apid].ttd > ttd ) return;
    this.maxTTD[apid] = {ttd, time, channel};
  }

  addCPGT(apid, cpgt, time, block) {
    if( !this.maxCPGT[apid] ) {
      this.maxCPGT[apid] = {cpgt, time, block};
      return;
    }
    if( this.maxCPGT[apid].cpgt > cpgt ) return;
    this.maxCPGT[apid] = {cpgt, time, block};
  }

  async write() {
    // write ttd metrics
    let data = this.maxTTD;
    this.maxTTD = {};

    for( let apid in data ) {
      let value = data[apid];
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

    // write cpgt metrics
    data = this.maxCPGT;
    this.maxCPGT = {};

    for( let apid in data ) {
      let value = data[apid];
      if( value === null || value === undefined ) continue;

      let dataPoint = {
        interval: {
          endTime: {
            seconds: value.time.getTime() / 1000,
          },
        },
        value: {
          int64Value: value.cpgt+'',
        },
      };

      let timeSeriesData = {
        metric: {
          type: this.TYPES.CPGT,
          labels: {
            apid, 
            block : value.block,
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
        logger.warn('error writing cpgt metric', e);
      }
    }
  }

}

module.exports = new Monitoring();