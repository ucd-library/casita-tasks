import {logger, config, KafkaProducer, RabbitMQ, sendMessage, waitUntil, Monitoring} from '@ucd-lib/casita-worker';
import metrics from '../../init/google-cloud-metrics.js';
import exec from './exec.js';
import moduleRunner from './module.js';
import {v4} from 'uuid';

const metricsDefs = {
  ttw : metrics.find(item => item.type === 'custom.googleapis.com/casita/time-to-worker'),
  exectime : metrics.find(item => item.type === 'custom.googleapis.com/casita/worker-execution-time'),
  status : metrics.find(item => item.type === 'custom.googleapis.com/casita/worker-execution-status')
}

let monitor = new Monitoring('casita-worker-'+(process.env.HOSTNAME || v4()));
monitor.registerMetric(metricsDefs.ttw);
monitor.registerMetric(metricsDefs.exectime);
monitor.registerMetric(metricsDefs.status);

// init kafka
// let kafkaConsumer = KafkaConsumer({
//   groupId : config.kafka.groups.worker,
//   heartbeatInterval	: 10 * 1000
// });
let rabbitMq = new RabbitMQ();
let kafkaProducer = KafkaProducer();

async function findAndSendMessage(stdout, topic) {
  stdout = stdout.split('\n');
  for( let line of stdout ) {
    try {
      line = JSON.parse(line);
      if( line.type !== 'casita-task-response' ) continue;

      let msg = {
        topic : line.topic || line.task || topic,
        source : line.source,
        data : line.data,
        external: line.external
      };

      logger.debug('Sending kafka task response message', msg);
      await sendMessage(msg, kafkaProducer);
    } catch(e) {};
  }
}

async function sendFromModule(module, data, args, topic) {
  try {
    let msg = {
      topic : topic,
      source : module,
      data : data,
      external: args.kafkaExternal ? true : false
    };

    logger.debug('Sending kafka task response message', msg);
    await sendMessage(msg, kafkaProducer);
  } catch(e) {}
}

(async function() {
  await waitUntil(config.kafka.host, config.kafka.port);
  // await kafkaConsumer.connect();
  await kafkaProducer.connect();

  await rabbitMq.connect();

  // logger.info(`Waiting for topic: ${config.kafka.topics.tasks}`);
  // await waitForTopics([config.kafka.topics.tasks]);

  // logger.info(`Topic ready ${config.kafka.topics.tasks}, subscribing`);
  // await kafkaConsumer.subscribe({
  //   topics: [config.kafka.topics.tasks]
  // });

  let lastMessageCompletedAt = Date.now();

  // await kafkaConsumer.run({
  //   eachMessage: async ({topic, partition, message, heartbeat, pause}) => {
  rabbitMq.listen(config.rabbitMq.queues.tasks, async msg => {
    try {
      msg.content = JSON.parse(msg.content.toString());

      // message.value = JSON.parse(message.value.toString())
      // logger.debug('casita worker received message: ', {topic, partition, offset: message.offset});
      logger.debug('rabbitmq message fetch time: ', (Date.now()-lastMessageCompletedAt));

      // time message was put on queue
      let timestamp = new Date(msg.content.time).getTime();
      monitor.setMaxMetric(
        metricsDefs.ttw.type,
        'task',
        Date.now() - timestamp,
        {
          task: msg.content.data.task
        }
      );

      // see how long the exec step takes and record
      timestamp = Date.now();
      let exitCode = 0;
      let cmd = msg.content.data.cmd;

      if( cmd.module ) {

        try {
          let data = await moduleRunner.run(cmd.module, cmd.args);
          await sendFromModule(cmd.module, data, cmd.args, msg.content.data.task);
        } catch(e) {
          logger.error('failed to run', msg.content.data, e);
          exitCode = 1;
        }

      } else {
        let response = await exec(cmd);
        await findAndSendMessage(response.stdout, msg.content.data.task);
        exitCode = response.exitCode;
      }

      monitor.setMaxMetric(
        metricsDefs.exectime.type,
        'task',
        Date.now() - timestamp,
        {
          task: msg.content.data.task
        }
      );

      monitor.incrementMetric(
        metricsDefs.status.type,
        'task',
        {
          task: msg.content.data.task,
          exitCode
        }
      );

    } catch(e) {
      logger.error('error processing queue message', e);
    }

    // TODO: we should have nack with default give up...
    await rabbitMq.ack(msg);
    lastMessageCompletedAt = Date.now();
  });

})()