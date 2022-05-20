const config = require('../../node-commons/config');
const logger = require('../../node-commons/logger');
const metrics = require('../../node-commons/metrics');

const metricsDefs = {
  time : {
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
  status : {
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
}

async function handleError(e, startTime) {
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

  logger.error(e);
  setTimeout(() => process.exit(100), 25);
}

function sendMetrics(time, labels) {
  metrics.registerMetric(metricsDefs.time);
  metrics.registerMetric(metricsDefs.status);

  metrics.write(metricsDefs.time, time, labels);
  metrics.write(metricsDefs.status, 1, labels);
}

/**
 * run command from config
 */
async function action(opts, cmd) {
  config.update(opts, cmd);

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
        command : config.command.current,
        status : 'success'
      }
    );
  }

  if( config.kafka.enabled ) {
    const kafka = require('../../node-commons/kafka');
    await kafka.connect();
    await kafka.send(resp);
    await kafka.flush(); // send message now
    await kafka.disconnect();
  }

  if( config.kafka.print === true ) {
    console.log(resp);
  }
}

module.exports = action;