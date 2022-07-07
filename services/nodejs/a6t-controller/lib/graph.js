import kafkaWorker from './kafka.js';
import pathUtils from 'path';
import fs from 'fs';
import {config} from '@ucd-lib/casita-worker';

const GENERIC_PAYLOAD_APIDS = /^(301|302)$/;

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
    dependencies : [TOPICS.productWriter],

    where : msg => ['image-fragment.jp2', 'fragment-metadata.json'].includes(msg.data.file.base),
    groupBy : msg => `${msg.data.scale}-${msg.data.date}T${msg.data.hour}:${msg.data.minsec}-${msg.data.x},${msg.data.y}-${msg.data.band}`,
    expire : 5, // 5 seconds
    ready : (key, msgs) => isBandReady(msgs),

    sink : (key, msgs) => {
      let {satellite, product, date, hour, minsec, file, band, apid, x, y} = msgs[0].data;

      let fmFile = pathUtils.join(config.fs.nfsRoot, satellite, product,
        date, hour, minsec, band, apid, 'blocks', x+'-'+y,
        'fragment-metadata.json');

      return kafkaWorker.exec(`${CASITA_CMD} image jp2-to-png -k ${TOPICS.blockCompositeImage} -m --metadata-file=${fmFile}`);

    }
  },

  [TOPICS.ringBuffer] : {
    enabled: false,
    dependencies : [TOPICS.blockCompositeImage],
    expire : 60 * 2, // 2 minutes
    where : msg => true,
    groupBy : msg => `${msg.data.product}-${msg.data.date}T${msg.data.hour}:${msg.data.minsec}-${msg.data.x},${msg.data.y}-${msg.data.band}`,
    sink : (key, msgs) => {
      let {satellite, product, date, hour, minsec, file, band, apid, x, y} = msgs[0].data;

      let pngFile = pathUtils.join(config.fs.nfsRoot, satellite, product,
        date, hour, minsec, band, apid, 'blocks', x+'-'+y,
        'image.png');

      return kafkaWorker.exec(`${CASITA_CMD} block-ring-buffer insert -k ${TOPICS.ringBuffer} -m --file=${pngFile}`);
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
      return kafkaWorker.exec(`casita image ca-project -k -m --product=${product} --time=${date}T${hour}:${minsec}`);
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

  'generic-payload-parser' : {
    enabled : false,
    dependencies : [config.kafka.topics.productWriter],

    where : data => (data.apid.match(GENERIC_PAYLOAD_APIDS) ? true : false) && (data.file.base === 'payload.bin'),

    sink : (key, msgs) => {
      let {product, date, hour, minsec, ms, band, apid, block, path} = msgs[0];

      return airflow.runDag(key, 'generic-payload-parser', {
        product, date, hour, minsec, ms, band, apid, block, path
      });
    }
  },

  'lighting-grouped-stats' : {
    enabled : false,
    expire : 30,
    where : () => false
  }


}

export default dag