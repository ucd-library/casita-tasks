import kafkaWorker from './kafka.js';
import pathUtils from 'path';
import fs from 'fs';
import {config} from '@ucd-lib/casita-worker';

const GENERIC_PAYLOAD_APIDS = /^(301|302)$/;


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

  [config.kafka.topics.blockCompositeImage] : {
    dependencies : [config.kafka.topics.productWriter],

    where : msg => ['image-fragment.jp2', 'fragment-metadata.json'].includes(msg.data.file.base),
    groupBy : msg => `${msg.data.scale}-${msg.data.date}T${msg.data.hour}:${msg.data.minsec}-${msg.data.x},${msg.data.y}-${msg.data.band}`,
    expire : 5,
    ready : (key, msgs) => isBandReady(msgs),

    sink : (key, msgs) => {
      let {satellite, scale, date, hour, minsec, file, band, apid, x, y} = msgs[0].data;

      let fmFile = pathUtils.join(config.fs.nfsRoot, satellite, scale,
        date, hour, minsec, band, apid, 'blocks', x+'-'+y,
        'fragment-metadata.json');

      // return kafkaWorker.exec(`casita image jp2-to-png -k -m --metadata-file=${fmFile}`);
      return kafkaWorker.exec(`node /casita/tasks/cli/casita.js image jp2-to-png -k ${config.kafka.topics.blockCompositeImage} -m --metadata-file=${fmFile}`);

    }
  },

  'ring-buffer' : {
    enabled: false,
    dependencies : [config.kafka.topics.blockCompositeImage],
    where : msg => true,
    groupBy : msg => `${msg.data.scale}-${msg.data.date}T${msg.data.hour}:${msg.data.minsec}-${msg.data.x},${msg.data.y}-${msg.data.band}`,

  },

  'ca-projection' : {
    enabled: false,
    dependencies : ['ring-buffer'],
    groupBy : msg => `${msg.data.scale}-${msg.data.date}T${msg.data.hour}:${msg.data.minsec}-${msg.data.band}`,
    where : msg => {
      let key = `${msg.data.x},${msg.data.y}`;
      return ['0-0','200-375'].includes(key);
    },

    sink : (key, msgs) => {
      let {satellite, scale, date, hour, minsec, file, band, apid, x, y} = msgs[0].data;
      return kafkaWorker.exec(`casita image ca-project -k -m --product=${scale} --time=${date}T${hour}:${minsec}`);
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
      let {scale, date, hour, minsec, ms, band, apid, block, path} = msgs[0];

      return airflow.runDag(key, 'generic-payload-parser', {
          scale, date, hour, minsec, ms, band, apid, block, path
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