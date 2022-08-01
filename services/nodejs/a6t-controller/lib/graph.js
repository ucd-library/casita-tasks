import kafkaWorker from './kafka.js';
import pathUtils from 'path';
import fs from 'fs';
import {config} from '@ucd-lib/casita-worker';

const LIGHTNING_PAYLOAD_APIDS = /^(301|302)$/;

// const CASITA_CMD = 'casita';
const CASITA_CMD = 'node /casita/tasks/cli/casita.js';
const TOPICS = config.kafka.topics;

function isBandReady(msgs) {
  let fragments = msgs.filter(item => item.data.file.base === 'image-fragment.jp2');
  let metadata = msgs.filter(item => item.data.file.base === 'fragment-metadata.json');
  
  if( !metadata.length ) return false;

  metadata = metadata[0].data.file;
  metadata = JSON.parse(fs.readFileSync(pathUtils.join(metadata.dir, metadata.base), 'utf-8'));
  
  return (metadata.fragmentsCount <= fragments.length);
}


const dag = {

  // [config.kafka.topics.productWriter] : {
  //   source : true
  // },

  [TOPICS.blockCompositeImage] : {
    enabled: true,
    dependencies : [TOPICS.productWriter],

    where : msg => ['image-fragment.jp2', 'fragment-metadata.json'].includes(msg.data.file.base),
    groupBy : msg => `${msg.data.product}-${msg.data.date}T${msg.data.hour}:${msg.data.minsec}-${msg.data.x},${msg.data.y}-${msg.data.band}`,
    expire : 5, // 5 seconds
    ready : (key, msgs) => isBandReady(msgs),

    sink : (key, msgs) => {
      let task = TOPICS.blockCompositeImage;
      let {satellite, product, date, hour, minsec, file, band, apid, x, y} = msgs[0].data;

      let fmFile = pathUtils.join(config.fs.nfsRoot, satellite, product,
        date, hour, minsec, band, apid, 'blocks', x+'-'+y,
        'fragment-metadata.json');

      return kafkaWorker.exec(`${CASITA_CMD} image jp2-to-png -p -e --metadata-file=${fmFile}`, {task, msgs});
    }
  },

  // no group by need here
  [TOPICS.ringBuffer] : {
    enabled: true,
    dependencies : [TOPICS.blockCompositeImage],
    where : msg => msg.data.band.match(/^(1|2|7)$/),
    sink : (key, msgs) => {
      let task = TOPICS.ringBuffer;
      let {satellite, product, date, hour, minsec, file, band, apid, x, y} = msgs[0].data;

      let pngFile = pathUtils.join(config.fs.nfsRoot, satellite, product,
        date, hour, minsec, band, apid, 'blocks', x+'-'+y,
        'image.png');

      return kafkaWorker.exec(`${CASITA_CMD} block-ring-buffer insert -p -e --file=${pngFile}`, {task, msgs});
    }
  },

  'ca-projection' : {
    enabled: false,
    dependencies : [config.kafka.topics.ringBuffer],
    groupBy : msg => `${msg.data.product}-${msg.data.date}T${msg.data.hour}:${msg.data.minsec}-${msg.data.band}`,
    where : msg => {
      return [
        '6664-852', '8332-852', '6664-1860','8332-1860', // conus
        '12656-2628', '14464-2628', '12656-3640', '14464-3640' // fulldisk
      ].includes(`${msg.data.x},${msg.data.y}`);
    },
    ready : (key, msgs) => msgs.length === 4,

    sink : (key, msgs) => {
      let {satellite, product, date, hour, minsec, file, band, apid, x, y} = msgs[0].data;
      return kafkaWorker.exec(`${CASITA_CMD} image ca-project -p -e --product=${product} --time=${date}T${hour}:${minsec}`, msgs);
    }
  },



  // 'full-composite-image' : {
  //   enabled : false,
  //   dependencies : ['block-composite-image'],

  //   groupBy : msg => `${msg.scale}-${msg.date}-${msg.hour}-${msg.minsec}-${msg.band}-${msg.apid}-${msg.file.base}`,
  //   where : data => ['image.png', 'web.png', 'web-scaled.png'].includes(data.file.base),
  //   expire : 60 * 20,

  //   ready : (key, msgs) => {
  //     let scale = msgs[0].scale;

  //     // TODO: wish there was a better way
  //     if( scale === 'mesoscale' && msgs.length >= 4 ) return true;
  //     if( scale === 'conus' && msgs.length >= 36 ) return true;
  //     if( scale === 'fulldisk' && msgs.length >= 229 ) return true;

  //     return false;
  //   },

  //   sink : (key, msgs) => {
  //     let {scale, date, hour, minsec, band, apid, block, path} = msgs[0];

  //     return airflow.runDag(key, 'full-composite-image', {
  //       scale, date, hour, minsec, band, apid, block, path
  //     });
  //   },
  // },

  [TOPICS.lightning] : {
    enabled : true,
    dependencies : [TOPICS.productWriter],

    groupBy : msg => `${msg.data.product}-${msg.data.date}-${msg.data.hour}-${msg.data.minsec}-${msg.data.ms}-${msg.data.apid}-${msg.data.file.base}`,
    where : msg => (msg.data.apid.match(LIGHTNING_PAYLOAD_APIDS)) && (msg.data.file.base === 'payload.bin'),
    ready : () => true,

    sink : (key, msgs) => {
      let task = TOPICS.lightning;
      let data = msgs[0].data;
      let file = pathUtils.resolve(data.file.dir, data.file.base);
      return kafkaWorker.exec(`${CASITA_CMD} generic parse-lightning -p -e --file=${file} `, {task, msgs});
    }
  },

  'lighting-grouped-stats' : {
    enabled : false,
    dependencies : [config.kafka.topics.lightning],
    enabled : false,
    expire : 30,
    where : () => false
  }


}

export default dag