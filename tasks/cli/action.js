import {config, logger} from '@ucd-lib/casita-worker';
import {update as updateConfig} from '../../node-commons/config.js';

async function handleError(e, startTime) {
  logger.error(e);
  setTimeout(() => process.exit(100), 25);
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
    console.log(JSON.stringify({
      type : 'casita-task-response',
      topic : config.kafka.topic,
      source : config.command.reference,
      data : resp,
      external: config.kafka.external
    }));
  }

  setTimeout(() => process.exit(), 25);
}

export default action;