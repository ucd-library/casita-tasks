import fs from 'fs-extra';
import path from 'path';
import {v4} from 'uuid';
import {logger, config, KafkaProducer} from '@ucd-lib/casita-worker';

let kafkaProducer = KafkaProducer();

async function send(productInfo, file, data) {
  try {

    productInfo = Object.assign({}, productInfo);
    file = path.join(config.fs.nfsRoot, file);
    productInfo.file = path.parse(file);

    fs.mkdirpSync(productInfo.file.dir);
    if( fs.existsSync(file) ) {
      fs.unlinkSync(file)
    }

    fs.writeFileSync(file, data);


    kafkaProducer.send({
      topic : config.kafka.topics.productWriter,
      messages : [{
        value : JSON.stringify({
          id : v4(),
          time : new Date().toISOString(),
          source : 'goes-product-writer',
          datacontenttype : 'application/json',
          data : productInfo
        })
      }]
    })

  } catch(e) {
    logger.error('Decoder krm interface failed to send subject: '+file, e);
  }
}

(async function() {
  await kafkaProducer.connect();
})()

export default send;