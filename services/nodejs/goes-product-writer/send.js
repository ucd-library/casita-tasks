import fs from 'fs-extra';
import {logger, config, KafkaProducer} from '@ucd-lib/casita-worker';

let kafkaProducer = new KafkaProducer();

async function send(file, data) {
  try {
    file = path.join(config.fs.nfsRoot, file);

    await fs.mkdirpSync(path.parse(file).dir);
    await fs.writeFile(file, data);

    kafkaProducer.client.produce({
      topic : config.kafka.topics.productWriter,
      value : {
        id : uuid.v4(),
        time : new Date().toISOString(),
        source : 'goes-product-writer',
        datacontenttype : 'application/json',
        data : {
          filePath: file
        }
      }
    })

  } catch(e) {
    logger.error('Decoder krm interface failed to send subject: '+file, e);
  }
}

(async function() {
  await kafkaProducer.client.connect();
})()

export default send;