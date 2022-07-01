import {config, KafkaProducer, sendMessage} from '@ucd-lib/casita-worker';

class CasitaKafkaWorkerExec {

  async connect() {
    if( !this.kafkaProducer ) {
      this.kafkaProducer = KafkaProducer();
      await this.kafkaProducer.connect();
    }
  }

  async exec(cmd) {
    if( typeof cmd === 'string' ) {
      cmd = {cmd}
    }

    await this.connect();
    await sendMessage({
      topic : config.kafka.topics.tasks,
      source : 'casita-a6t',
      data : cmd
    });

    return {success: true};
  }

}

const instance = new CasitaKafkaWorkerExec();
export default instance;