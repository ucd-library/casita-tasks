import {config, logger, Monitoring} from '@ucd-lib/casita-worker';
import {update as updateConfig} from '../../node-commons/config.js';
// const logger = require('../../node-commons/logger');
import metrics from '../../services/init/google-cloud-metrics.js';

const metricsDefs = {
  time : metrics.find(item => item.type === 'custom.googleapis.com/casita/worker-execution-time'),
  status : metrics.find(item => item.type === 'custom.googleapis.com/casita/worker-execution-status')
}
const monitor = new Monitoring('casita-cli');

async function handleError(e, startTime) {
  logger.error(e);

  if( config.metrics ) {
    await sendMetrics(
      startTime-Date.now(),
      {
        command : config.command.current,
        status : 'error'
      }
    );
    
    setTimeout(() => process.exit(100), 50);
    return;
  }

  setTimeout(() => process.exit(100), 25);
}

function sendMetrics(time, labels) {
  monitor.registerMetric(metricsDefs.time);
  monitor.registerMetric(metricsDefs.status);

  metrics.write(metricsDefs.time, time, labels);
  // metrics.write(metricsDefs.status, 1, labels);
}

/**
 * run command from config
 */
async function action(opts, cmd) {
  updateConfig(opts, cmd);

  if( config.debugConfig === true) {
    logger.info(config);
  }

  let startTime = Date.now();

  let resp;
  try {
    let module = await import(config.command.reference);
    if( module.default ) module = module.default;
  
    if( typeof module === 'function') {
      resp = await module();
    } else {
      resp = module;
    }

  } catch(e) {
    handleError(e, startTime);
    return;
  }

  if( config.metrics ) {
    await sendMetrics(
      startTime-Date.now(),
      {
        command : config.command,
        status : 'success'
      }
    );
  }

  if( config.kafka.topic ) {
    const {sendMessage} = await import('../../node-commons/kafka.js');
    const {kafkaProducer} = await sendMessage({
      topic : config.kafka.topic,
      source : config.command.reference,
      data : resp,
      external: config.kafka.external
    });
    await kafkaProducer.disconnect();
  }

  if( config.kafka.print === true ) {
    console.log(resp);
  }
}

export default action;