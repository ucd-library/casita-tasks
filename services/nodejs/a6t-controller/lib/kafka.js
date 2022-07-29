import {config, KafkaProducer, sendMessage, Monitoring} from '@ucd-lib/casita-worker';
import metrics from '../../../init/google-cloud-metrics.js';

const METRIC_TYPE = 'custom.googleapis.com/casita/a6t-compose-time';
let metric = metrics.find(item => item.type === METRIC_TYPE);

// init monitoring
let monitor = new Monitoring('argonaut');
monitor.registerMetric(metric);

class CasitaKafkaWorkerExec {

  async connect() {
    if( !this.kafkaProducer ) {
      this.kafkaProducer = KafkaProducer();
      await this.kafkaProducer.connect();
    }
  }

  async exec(cmd, metadata) {
    if( typeof cmd === 'string' ) {
      cmd = {cmd}
    }

    // get earliest time for all messages
    let timestamp = metadata.msgs[0].time;
    metadata.msgs.forEach(msg => {
      if( msg.time < timestamp ) timestamp = msg.time;
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
    await sendMessage({
      topic : config.kafka.topics.tasks,
      source : 'argonaut',
      data : {cmd, task: metadata.task}
    });

    return {success: true};
  }

}

const instance = new CasitaKafkaWorkerExec();
export default instance;