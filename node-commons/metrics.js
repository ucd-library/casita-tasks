import config from './config.js';
import logger from './logger.js';
import monitoring from '@google-cloud/monitoring';

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

    if( !config.google.applicationCredentials ) {
      this.enabled = false;
      logger.warn('No GOOGLE_APPLICATION_CREDENTIALS set. metrics will not write');
    } else {
      this.enabled = true;
      setInterval(() => this.write(), this.interval);
    }
  }

  registerMetric(metric, opts={}) {
    if( !this.enabled ) return;

    if( !metric.metricDescriptor ) {
      metric = {
        name : this.client.projectPath(config.google.projectId),
        metricDescriptor : metric
      }
    }

    this.metrics[metric.metricDescriptor.type] = {metric, opts};
    this.data[metric.metricDescriptor.type] = {};
  }

  setMaxMetric(type, key, value, args={}) {
    if( !this.enabled ) return;

    let current = this.getMetricValue(type, args[key]);

    if( !current ) {
      this.setMetricValue(type, key, value, args);
      return true;
    }

    if( current.value > value ) return false;

    this.setMetricValue(type, key, value, args);
    return true;
  }

  incrementMetric(type, key, args, value) {
    if( !this.enabled ) return;

    let current = this.getMetricValue(type, args[key]);
    if( value === undefined ) value = 1;

    if( !current ) {
      this.setMetricValue(type, key, value, args);
      return true;
    }
    this.setMetricValue(type, key, current.value+value, args);
    return true;
  }

  setMetricValue(type, key, value, args={}) {
    if( !this.enabled ) return;
    
    if( !args[key] ) throw new Error('Metric args does not contain key: '+key);
    if( !this.data[type] ) throw new Error('Unknown metric type: '+type);
    
    args.value = value;
    
    this.data[type][args[key]] = args;

    logger.debug(`setting metric ${type} ${args[key]}`, args);
  }

  getMetricValue(type, key) {
    if( !this.data[type] ) throw new Error('Unknown metric type: '+type);
    return this.data[type][key];
  }

  // async ensureMetrics() {
  //   if( !this.enabled ) return;

  //   for( let key in this.metrics ) {
  //     logger.info('Ensuring metric: ', key);
  //     await this.ensureMetric(this.metrics[key].metric);
  //   }
  // }

  // ensureMetric(metric) {
  //   if( !config.google.applicationCredentials ) {
  //     return;
  //   }

  //   return this.client.createMetricDescriptor(metric);
  // }

  write() {
    for( let type in this.data ) {
      for( let stat in this.data[type] ) {
        let labels = this.data[type][stat];
        let value = labels.value;
        delete labels.value;

        this._write(type, value, labels);
      }
      this.data[type] = {};
    }
  }
  

  async _write(type, value, labels) {
    if( !this.enabled ) return;

    let opts = this.metrics[type].opts;
    if( opts.average === true ) {
      value = value / 30;
    }

    value = parseInt(Math.round(value));

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
          project_id: config.google.projectId
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
      logger.debug(`sendig metric ${type}`, {value:dataPoint.value}, labels);
      let result = await this.client.createTimeSeries(request);
      logger.debug(`metric create result ${type}`, result)
    } catch(e) {
      logger.warn(`error writing metric ${type}`, e);
    }
  }

}


export default Monitoring;