import airflow from './airflow.js';
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

  'block-composite-image' : {
    dependencies : [config.kafka.topics.productWriter],

    where : msg => ['image-fragment.jp2', 'fragment-metadata.json'].includes(msg.data.file.base),
    groupBy : msg => `${msg.data.scale}-${msg.data.date}T${msg.data.hour}:${msg.data.minsec}-${msg.data.x},${msg.data.y}`,
    expire : 60 * 2,
    
    ready : (key, msgs) => {
      // group by band for airflow tasks
      let bands = {}, band;
      msgs.forEach(msg => {
        if( !bands[msg.data.band] ) bands[msg.data.band] = []
        bands[msg.data.band].push(msg);
      })

      let pendingTime = new Date(msgs[0].time).getTime();
      if( (Date.now() - pendingTime < 5000) && Object.keys(bands).length < 16 ) {
        return;
      }

      for( let bandID in bands ) {
        band = bands[bandID];
        if( !isBandReady(band) ) {
          return false;
        }
      }

      console.log(key, Date.now() - pendingTime, Object.keys(bands) );

      return true;
    },

    sink : (key, msgs) => {
      let {satellite, scale, date, hour, minsec, block, file, apid, x, y} = msgs[0].data;

      let files = {};
      msgs.forEach(msg => {
        files[parseInt(msg.data.band)] = pathUtils.join(config.fs.nfsRoot, satellite, scale,
          date, hour, minsec, msg.data.band, msg.data.apid, 'blocks', x+'-'+y,
          'fragment-metadata.json');
      });
      // console.log(files);

      return airflow.runDag(
        key+'-'+Object.keys(files).join(':')+'-'+Date.now(), 
        'block-composite-images', 
        // 'block-test', 
        {files}
      );
    }
  },



  'full-composite-image' : {
    enabled : false,
    dependencies : ['block-composite-images'],

    groupBy : msg => `${msg.scale}-${msg.date}-${msg.hour}-${msg.minsec}-${msg.band}-${msg.apid}-${msg.file.base}`,
    where : data => ['image.png', 'web.png', 'web-scaled.png'].includes(data.file.base),
    expire : 60 * 20,

    ready : (key, msgs) => {
      let scale = msgs[0].scale;

      // TODO: wish there was a better way
      if( scale === 'mesoscale' && msgs.length >= 4 ) return true;
      if( scale === 'conus' && msgs.length >= 36 ) return true;
      if( scale === 'fulldisk' && msgs.length >= 229 ) return true;

      return false;
    },

    sink : (key, msgs) => {
      let {scale, date, hour, minsec, band, apid, block, path} = msgs[0];

      return airflow.runDag(key, 'full-composite-image', {
        scale, date, hour, minsec, band, apid, block, path
      });
    },
  },

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
    enabled : false
  }


}

export default dag