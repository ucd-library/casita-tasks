import {config, RabbitMQ, Monitoring} from '@ucd-lib/casita-worker';
import metrics from '../../../init/google-cloud-metrics.js';
import {v4} from 'uuid';
import logger from '../../../../node-commons/logger.js';

const METRIC_TYPE = 'custom.googleapis.com/casita/a6t-compose-time';
let metric = metrics.find(item => item.type === METRIC_TYPE);

// init monitoring
let monitor = new Monitoring('argonaut');
monitor.registerMetric(metric);

class CasitaRabbitMqWorkerExec {

  async connect() {
    if( !this.rabbitMq ) {
      this.rabbitMq = new RabbitMQ();
      await this.rabbitMq.connect();

      // TODO: move this to init container...
      logger.info('Ensuring queue: ', config.rabbitMq.queues.tasks);
      await this.rabbitMq.createQueues(config.rabbitMq.queues.tasks);
    }
  }

  async exec(cmd, metadata) {
    if( typeof cmd === 'string' ) {
      cmd = {cmd}
    }

    // get earliest time for all messages
    let timestamp = new Date(metadata.msgs[0].time).getTime();
    metadata.msgs.forEach(msg => {
      if( new Date(msg.time).getTime() < timestamp ) timestamp = new Date(msg.time).getTime();
    });

    // send a6t compose time metric
    monitor.setMaxMetric(
      METRIC_TYPE,
      'task', 
      Date.now() - timestamp,
      {
        task: metadata.task
      }
    );

    await this.connect();
    
    this.rabbitMq.send(
      config.rabbitMq.queues.tasks,
      {
        id : v4(),
        time : new Date().toISOString(),
        source : 'argonaut',
        datacontenttype : 'application/json',
        data : {cmd, task: metadata.task}
      }
    )

    return {success: true};
  }

}

const instance = new CasitaRabbitMqWorkerExec();
export default instance;