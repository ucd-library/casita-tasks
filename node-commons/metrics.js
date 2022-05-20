const config = require('./config');
const logger = require('./logger');
const fs = require('fs')
const monitoring = require('@google-cloud/monitoring');

// https://cloud.google.com/monitoring/custom-metrics/creating-metrics

/** 
 * There is no cli for metric description removal (dump).  Here is a quick script
 * 
 * const monitoring = require('@google-cloud/monitoring');
 * let client = new monitoring.MetricServiceClient({keyFilename: '/etc/google/service-account.json'})
 * client.deleteMetricDescriptor({name:'projects/digital-ucdavis-edu/metricDescriptors/custom.googleapis.com/krm/tasks_ready'}).then(e => console.log(e))
 */

class Monitoring {

  constructor(serviceId) {
    if( !serviceId ) throw new Error('You must set a monitor id');
    this.serviceId = serviceId;
    
    let clientConfig = {
      keyFilename : config.google.applicationCredentials
    };
    this.client = new monitoring.MetricServiceClient(clientConfig);

    this.metrics = {};
    this.data = {};

    this.interval = 1000 * 30;
    this.startTime = new Date();
  }

  registerMetric(metric, opts={}) {
    if( !metric.metricDescriptor ) {
      metric = {
        name : this.client.projectPath(config.google.projectId),
        metricDescriptor : metric
      }
    }

    this.metrics[metric.metricDescriptor.type] = {metric, opts};
    this.data[metric.metricDescriptor.type] = {};
  }

  async ensureMetrics() {
    for( let key in this.metrics ) {
      logger.info('Ensuring metric: ', key);
      await this.ensureMetric(this.metrics[key].metric);
    }
  }

  ensureMetric(metric) {
    return this.client.createMetricDescriptor(metric);
  }


  async write(type, value, labels) {
    let dataPoint = {
      interval: {
        // startTime : {
        //   seconds: startTime.getTime() / 1000
        // },
        endTime: {
          seconds: new Date().getTime() / 1000
        }
      },
      value: {
        int64Value: value+'',
      },
    };


    labels.instance = config.instance;


    let timeSeriesData = {
      metric: {type, labels},
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
      logger.debug(`sendig metric ${type} ${key}`, {value:item.value}, labels);
      let result = await this.client.createTimeSeries(request);
      logger.debug(`metric create result ${type} ${key}`, result)
    } catch(e) {
      logger.warn(`error writing metric ${type} ${key}`, e);
    }
  }

}


module.exports = new Monitoring('casita-worker-'+Date.now());